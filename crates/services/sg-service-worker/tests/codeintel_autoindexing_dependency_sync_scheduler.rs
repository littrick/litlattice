#![forbid(unsafe_code)]

//! M9.8dh differential harness (Rust side): the sync-queue half of
//! `codeintel-autoindexing-dependency-scheduler`
//! (`dependencySyncSchedulerHandler.Handle`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/background/dependencies/
//! sync_scheduler_harness_test.go`. One canonical `dss ` JSON line per case:
//! handler error (multi-errors sorted and joined with ` | `), package-repo-ref
//! insertions, inserted dependency-indexing jobs, listed kinds/upserts and
//! call counts. Kind lists, inserted jobs and error texts are sorted because
//! the baseline iterates a Go map. Volatile timestamps are reduced to
//! presence/zero markers.

use std::sync::Mutex;

use async_trait::async_trait;
use sg_db::codeintel_dependencies::{PackageNameFilter, PackageRepoFilter, PackageVersionFilter};
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::codeintel_autoindexing_dependency_sync_scheduler::{
    new_package, DependencyIndexingJobStore, DependencySyncDependencies,
    DependencySyncExternalServices, DependencySyncSchedulerHandler, DependencySyncUploads,
    DependencySyncingJob, ExternalService, HandlerSettings, InsertedPackageRepoRefs,
    MinimalPackageRepoRef, Package, PackageReferences, SyncSchedulerError, Upload,
};

#[derive(serde::Serialize)]
struct DssLine {
    case: &'static str,
    error: String,
    deps_inserts: Vec<String>,
    inserted_jobs: Vec<String>,
    ext_kinds: Vec<String>,
    upserts: Vec<String>,
    list_calls: usize,
    upload_calls: usize,
    references_calls: usize,
}

fn emit(prefix: &str, value: &DssLine) {
    println!(
        "{prefix} {}",
        serde_json::to_string(value).expect("json snapshot")
    );
}

fn error_text(error: &Option<SyncSchedulerError>) -> String {
    match error {
        None => "-".to_owned(),
        Some(SyncSchedulerError::Message(message)) => message.clone(),
        Some(SyncSchedulerError::Multiple(messages)) => {
            let mut messages = messages.clone();
            messages.sort();
            messages.join(" | ")
        }
    }
}

struct FakeUploads {
    references: Vec<Package>,
    references_error: Option<String>,
    next_error: Option<String>,
    close_error: Option<String>,
    upload: Option<Upload>,
    upload_error: Option<String>,
    calls: Mutex<usize>,
    upload_calls: Mutex<usize>,
}

#[async_trait]
impl DependencySyncUploads for FakeUploads {
    async fn references_for_upload(&self, _upload_id: i32) -> Result<PackageReferences, String> {
        *self.calls.lock().unwrap() += 1;
        if let Some(error) = &self.references_error {
            return Err(error.clone());
        }
        Ok(PackageReferences {
            references: self.references.clone(),
            next_error: self.next_error.clone(),
            close_error: self.close_error.clone(),
        })
    }

    async fn get_upload_by_id(&self, _upload_id: i32) -> Result<Option<Upload>, String> {
        *self.upload_calls.lock().unwrap() += 1;
        if let Some(error) = &self.upload_error {
            return Err(error.clone());
        }
        Ok(self.upload.clone())
    }
}

struct FakeDependencies {
    filters: Vec<PackageRepoFilter>,
    filters_error: Option<String>,
    existing_repo: bool,
    existing_version: bool,
    insert_fail_at: Vec<usize>,
    insert_calls: Mutex<usize>,
    recorded: Mutex<Vec<MinimalPackageRepoRef>>,
}

#[async_trait]
impl DependencySyncDependencies for FakeDependencies {
    async fn list_package_repo_filters(&self) -> Result<Vec<PackageRepoFilter>, String> {
        match &self.filters_error {
            Some(error) => Err(error.clone()),
            None => Ok(self.filters.clone()),
        }
    }

    async fn insert_package_repo_refs(
        &self,
        refs: &[MinimalPackageRepoRef],
    ) -> Result<InsertedPackageRepoRefs, String> {
        let mut calls = self.insert_calls.lock().unwrap();
        let call = *calls;
        *calls += 1;
        self.recorded.lock().unwrap().extend_from_slice(refs);
        if self.insert_fail_at.contains(&call) {
            return Err("insert-boom".to_owned());
        }
        Ok(InsertedPackageRepoRefs {
            new_repo_refs: usize::from(!self.existing_repo),
            new_versions: usize::from(!self.existing_version),
        })
    }
}

struct FakeStore {
    job_fail_kinds: Vec<String>,
    calls: Mutex<Vec<(String, Option<OffsetDateTime>)>>,
}

#[async_trait]
impl DependencyIndexingJobStore for FakeStore {
    async fn insert_dependency_indexing_job(
        &self,
        _upload_id: i32,
        external_service_kind: &str,
        sync_time: Option<OffsetDateTime>,
    ) -> Result<i32, String> {
        self.calls
            .lock()
            .unwrap()
            .push((external_service_kind.to_owned(), sync_time));
        if self
            .job_fail_kinds
            .iter()
            .any(|kind| kind == external_service_kind)
        {
            return Err("job-boom".to_owned());
        }
        Ok(1)
    }
}

struct FakeExternalServices {
    services: Vec<ExternalService>,
    list_error: Option<String>,
    upsert_fail_at: Vec<usize>,
    list_calls: Mutex<usize>,
    listed_kinds: Mutex<Vec<String>>,
    upsert_calls: Mutex<usize>,
    upserts: Mutex<Vec<(i64, String, bool)>>,
}

#[async_trait]
impl DependencySyncExternalServices for FakeExternalServices {
    async fn list(&self, kinds: &[String]) -> Result<Vec<ExternalService>, String> {
        let mut calls = self.list_calls.lock().unwrap();
        *calls += 1;
        self.listed_kinds.lock().unwrap().extend_from_slice(kinds);
        match &self.list_error {
            Some(error) => Err(error.clone()),
            None => Ok(self.services.clone()),
        }
    }

    async fn upsert_next_sync_at(
        &self,
        service: &ExternalService,
        next_sync_at: OffsetDateTime,
    ) -> Result<(), String> {
        let mut calls = self.upsert_calls.lock().unwrap();
        let call = *calls;
        *calls += 1;
        self.upserts.lock().unwrap().push((
            service.id,
            service.display_name.clone(),
            next_sync_at != OffsetDateTime::UNIX_EPOCH,
        ));
        if self.upsert_fail_at.contains(&call) {
            return Err("upsert-boom".to_owned());
        }
        Ok(())
    }
}

fn package(scheme: &str, name: &str, version: &str) -> Package {
    Package {
        scheme: scheme.to_owned(),
        manager: String::new(),
        name: name.to_owned(),
        version: version.to_owned(),
    }
}

fn jvm(name: &str, version: &str) -> Package {
    package("semanticdb", name, version)
}

const SCIP_JAVA: &str = "scip-java";

struct Case {
    name: &'static str,
    references: Vec<Package>,
    references_error: Option<&'static str>,
    next_error: Option<&'static str>,
    close_error: Option<&'static str>,
    upload: Option<Upload>,
    upload_error: Option<&'static str>,
    filters: Vec<PackageRepoFilter>,
    filters_error: Option<&'static str>,
    existing_repo: bool,
    existing_version: bool,
    insert_fail_at: Vec<usize>,
    services: Vec<ExternalService>,
    list_error: Option<&'static str>,
    upsert_fail_at: Vec<usize>,
    job_fail_kinds: Vec<&'static str>,
}

fn base(name: &'static str) -> Case {
    Case {
        name,
        references: Vec::new(),
        references_error: None,
        next_error: None,
        close_error: None,
        upload: Some(Upload {
            id: 42,
            indexer: SCIP_JAVA.to_owned(),
        }),
        upload_error: None,
        filters: Vec::new(),
        filters_error: None,
        existing_repo: false,
        existing_version: false,
        insert_fail_at: Vec::new(),
        services: Vec::new(),
        list_error: None,
        upsert_fail_at: Vec::new(),
        job_fail_kinds: Vec::new(),
    }
}

fn service(id: i64, display_name: &str) -> ExternalService {
    ExternalService {
        id,
        display_name: display_name.to_owned(),
    }
}

fn name_filter(id: i32, behaviour: &str, scheme: &str, glob: &str) -> PackageRepoFilter {
    PackageRepoFilter {
        id,
        behaviour: behaviour.to_owned(),
        package_scheme: scheme.to_owned(),
        name_filter: Some(PackageNameFilter {
            package_glob: glob.to_owned(),
        }),
        version_filter: None,
    }
}

fn version_filter(
    id: i32,
    behaviour: &str,
    scheme: &str,
    package_name: &str,
    version_glob: &str,
) -> PackageRepoFilter {
    PackageRepoFilter {
        id,
        behaviour: behaviour.to_owned(),
        package_scheme: scheme.to_owned(),
        name_filter: None,
        version_filter: Some(PackageVersionFilter {
            package_name: package_name.to_owned(),
            version_glob: version_glob.to_owned(),
        }),
    }
}

fn package_key(reference: &MinimalPackageRepoRef) -> String {
    let version = reference.versions.first();
    let version_text = version
        .map(|version| version.version.as_str())
        .unwrap_or("");
    let version_blocked = version.map(|version| version.blocked).unwrap_or(false);
    let version_checked = version
        .map(|version| version.last_checked_at.is_some())
        .unwrap_or(false);
    format!(
        "{}|{}|blocked={}|checked={}|{}|blocked={}|checked={}",
        reference.scheme,
        reference.name,
        reference.blocked,
        reference.last_checked_at.is_some(),
        version_text,
        version_blocked,
        version_checked
    )
}

#[tokio::test]
async fn autoindexing_dependency_sync_scheduler_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_DEPENDENCY_SYNC_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }

    let mut cases = Vec::new();

    let mut c = base("jvm-basic");
    c.references = vec![jvm("name1", "v2.2.0")];
    cases.push(c);

    let mut c = base("jvm-existing");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.existing_repo = true;
    c.existing_version = true;
    cases.push(c);

    let mut c = base("jvm-insert-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.insert_fail_at = vec![0];
    cases.push(c);

    let mut c = base("jvm-upsert-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.services = vec![service(7, "jvm-service")];
    c.upsert_fail_at = vec![0];
    cases.push(c);

    let mut c = base("jvm-list-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.list_error = Some("list-boom");
    cases.push(c);

    let mut c = base("jvm-list-error-with-insert-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.insert_fail_at = vec![0];
    c.list_error = Some("list-boom");
    cases.push(c);

    let mut c = base("gomod-only");
    c.upload = Some(Upload {
        id: 42,
        indexer: "lsif-go".to_owned(),
    });
    c.references = vec![package("gomod", "github.com/tsenart/vegeta/v12", "12.7.0")];
    cases.push(c);

    let mut c = base("gomod-non-indexer");
    c.upload = Some(Upload {
        id: 42,
        indexer: "lsif-node".to_owned(),
    });
    c.references = vec![package("gomod", "github.com/tsenart/vegeta/v12", "12.7.0")];
    cases.push(c);

    cases.push(base("no-references"));

    let mut c = base("references-error");
    c.references_error = Some("refs-boom");
    cases.push(c);

    let mut c = base("filters-error");
    c.filters_error = Some("filters-boom");
    cases.push(c);

    let mut c = base("invalid-npm");
    c.references = vec![package(
        "npm",
        "@automapper/classes/transformer-plugin",
        "0.24.0",
    )];
    cases.push(c);

    let mut c = base("npm-valid");
    c.references = vec![package("npm", "@graphql-mesh/graphql", "0.24.0")];
    cases.push(c);

    let mut c = base("scip-typescript-rewritten");
    c.references = vec![package("scip-typescript", "left-pad", "1.0.0")];
    cases.push(c);

    let mut c = base("scip-python-rewritten");
    c.references = vec![package("scip-python", "requests", "2.0.0")];
    cases.push(c);

    let mut c = base("jvm-name-normalization");
    c.references = vec![jvm("maven/junit/junit", "4.2")];
    cases.push(c);

    let mut c = base("multi-schemes");
    c.references = vec![jvm("name1", "v2.2.0"), package("npm", "left-pad", "1.0.0")];
    c.services = vec![service(7, "jvm-service"), service(8, "npm-service")];
    cases.push(c);

    let mut c = base("get-upload-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.upload_error = Some("upload-boom");
    cases.push(c);

    let mut c = base("upload-missing");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.upload = None;
    cases.push(c);

    let mut c = base("close-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.close_error = Some("close-boom");
    cases.push(c);

    let mut c = base("next-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.next_error = Some("next-boom");
    cases.push(c);

    let mut c = base("job-insert-error");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.job_fail_kinds = vec!["JVMPACKAGES"];
    cases.push(c);

    let mut c = base("filter-blocks-name");
    c.references = vec![jvm("name1", "v2.2.0")];
    c.filters = vec![name_filter(1, "BLOCK", "semanticdb", "name1")];
    cases.push(c);

    let mut c = base("filter-allows-version");
    c.upload = Some(Upload {
        id: 42,
        indexer: "lsif-java".to_owned(),
    });
    c.references = vec![jvm("junit:junit", "4.2")];
    c.filters = vec![version_filter(
        2,
        "ALLOW",
        "semanticdb",
        "junit:junit",
        "4.*",
    )];
    cases.push(c);

    assert_eq!(cases.len(), 24);

    for case in &cases {
        let uploads = std::sync::Arc::new(FakeUploads {
            references: case.references.clone(),
            references_error: case.references_error.map(str::to_owned),
            next_error: case.next_error.map(str::to_owned),
            close_error: case.close_error.map(str::to_owned),
            upload: case.upload.clone(),
            upload_error: case.upload_error.map(str::to_owned),
            calls: Mutex::new(0),
            upload_calls: Mutex::new(0),
        });
        let dependencies = std::sync::Arc::new(FakeDependencies {
            filters: case.filters.clone(),
            filters_error: case.filters_error.map(str::to_owned),
            existing_repo: case.existing_repo,
            existing_version: case.existing_version,
            insert_fail_at: case.insert_fail_at.clone(),
            insert_calls: Mutex::new(0),
            recorded: Mutex::new(Vec::new()),
        });
        let store = std::sync::Arc::new(FakeStore {
            job_fail_kinds: case
                .job_fail_kinds
                .iter()
                .map(|k| (*k).to_owned())
                .collect(),
            calls: Mutex::new(Vec::new()),
        });
        let external_services = std::sync::Arc::new(FakeExternalServices {
            services: case.services.clone(),
            list_error: case.list_error.map(str::to_owned),
            upsert_fail_at: case.upsert_fail_at.clone(),
            list_calls: Mutex::new(0),
            listed_kinds: Mutex::new(Vec::new()),
            upsert_calls: Mutex::new(0),
            upserts: Mutex::new(Vec::new()),
        });

        let handler = DependencySyncSchedulerHandler {
            uploads: uploads.clone(),
            dependencies: dependencies.clone(),
            store: store.clone(),
            external_services: external_services.clone(),
            settings: HandlerSettings::default(),
        };

        let result = handler
            .handle(&DependencySyncingJob {
                id: 101,
                upload_id: 42,
            })
            .await;

        let mut deps_inserts: Vec<String> = dependencies
            .recorded
            .lock()
            .unwrap()
            .iter()
            .map(package_key)
            .collect();
        deps_inserts.sort();

        let mut inserted_jobs: Vec<String> = store
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(kind, sync)| format!("{}|{}", kind, if sync.is_some() { "set" } else { "zero" }))
            .collect();
        inserted_jobs.sort();

        let mut ext_kinds = external_services.listed_kinds.lock().unwrap().clone();
        ext_kinds.sort();

        let mut upserts: Vec<String> = external_services
            .upserts
            .lock()
            .unwrap()
            .iter()
            .map(|(id, display_name, next_set)| {
                format!(
                    "{}|{}|next={}",
                    id,
                    display_name,
                    if *next_set { "set" } else { "zero" }
                )
            })
            .collect();
        upserts.sort();

        emit(
            "dss",
            &DssLine {
                case: case.name,
                error: error_text(&result.err()),
                deps_inserts,
                inserted_jobs,
                ext_kinds,
                upserts,
                list_calls: *external_services.list_calls.lock().unwrap(),
                upload_calls: *uploads.upload_calls.lock().unwrap(),
                references_calls: *uploads.calls.lock().unwrap(),
            },
        );
    }

    // Keep the unit-level helper compiled in the harness target too.
    assert_eq!(
        new_package(&package("semanticdb", "maven/a/b", "1"))
            .unwrap()
            .name,
        "a:b"
    );
}
