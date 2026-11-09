#![forbid(unsafe_code)]

//! M9.8dg differential harness (Rust side): the index-queue half of
//! `codeintel-autoindexing-dependency-scheduler`
//! (`dependencyIndexingSchedulerHandler.Handle`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/background/dependencies/
//! indexing_scheduler_harness_test.go`. One canonical `dis ` JSON line per
//! case: handler error (multi-errors sorted and joined with ` | `), requeued
//! ids, enqueued packages, and the recorded calls into the injected stores.
//! Enqueued packages and error texts are sorted because the baseline iterates
//! a Go map when enqueuing.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use sg_db::gitserver_repos::CloneStatus;
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::codeintel_autoindexing_dependency_indexing_scheduler::{
    set_difference, DependencyIndexingJob, DependencyIndexingSchedulerHandler,
    DependencyIndexingWorkerStore, ExternalService, ExternalServiceStore, GitserverRepoStore,
    HandleError, HandlerSettings, IndexEnqueuer, PackageReference, ReposStore, UploadService,
};
use sg_service_worker::jobs::codeintel_autoindexing_enqueuer::VersionedPackageRepo;

#[derive(serde::Serialize)]
struct DisLine {
    case: &'static str,
    error: String,
    requeued: Vec<i64>,
    enqueued: Vec<String>,
    ext_kinds: Vec<String>,
    listed_names: Vec<String>,
    gitserver_names: Vec<String>,
    references_calls: usize,
}

fn emit(prefix: &str, value: &DisLine) {
    println!(
        "{prefix} {}",
        serde_json::to_string(value).expect("json snapshot")
    );
}

struct FakeUploads {
    references: Vec<PackageReference>,
    error: Option<String>,
    calls: Mutex<usize>,
}

#[async_trait]
impl UploadService for FakeUploads {
    async fn references_for_upload(
        &self,
        _upload_id: i32,
    ) -> Result<Vec<PackageReference>, String> {
        *self.calls.lock().unwrap() += 1;
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.references.clone()),
        }
    }
}

struct FakeRepos {
    listed: Vec<String>,
    error: Option<String>,
    calls: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl ReposStore for FakeRepos {
    async fn list_minimal_repos(&self, names: &[String]) -> Result<Vec<String>, String> {
        self.calls.lock().unwrap().push(names.to_vec());
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.listed.clone()),
        }
    }
}

struct FakeExternalServices {
    services: Vec<ExternalService>,
    error: Option<String>,
    calls: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl ExternalServiceStore for FakeExternalServices {
    async fn list(&self, kinds: &[String]) -> Result<Vec<ExternalService>, String> {
        self.calls.lock().unwrap().push(kinds.to_vec());
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.services.clone()),
        }
    }
}

struct FakeGitserver {
    repos: HashMap<String, CloneStatus>,
    error: Option<String>,
    calls: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl GitserverRepoStore for FakeGitserver {
    async fn get_by_names(&self, names: &[String]) -> Result<HashMap<String, CloneStatus>, String> {
        self.calls.lock().unwrap().push(names.to_vec());
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.repos.clone()),
        }
    }
}

struct FakeEnqueuer {
    errors: HashMap<String, String>,
    calls: Mutex<Vec<VersionedPackageRepo>>,
}

fn package_key(package: &VersionedPackageRepo) -> String {
    format!("{}|{}|{}", package.scheme, package.name, package.version)
}

#[async_trait]
impl IndexEnqueuer for FakeEnqueuer {
    async fn queue_auto_index_jobs_for_package(
        &self,
        package: &VersionedPackageRepo,
    ) -> Result<(), String> {
        self.calls.lock().unwrap().push(package.clone());
        match self.errors.get(&package_key(package)) {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

struct FakeWorkerStore {
    requeued: Mutex<Vec<i64>>,
}

#[async_trait]
impl DependencyIndexingWorkerStore for FakeWorkerStore {
    async fn requeue(&self, id: i64, _process_after: OffsetDateTime) -> Result<(), String> {
        self.requeued.lock().unwrap().push(id);
        Ok(())
    }
}

fn reference(scheme: &str, name: &str, version: &str) -> PackageReference {
    PackageReference {
        scheme: scheme.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
    }
}

fn gomod(name: &str, version: &str) -> PackageReference {
    reference("gomod", name, version)
}

struct Case {
    name: &'static str,
    auto_indexing: bool,
    disabled: bool,
    ext_kind: &'static str,
    ext_sync: Option<OffsetDateTime>,
    references: Vec<PackageReference>,
    references_error: Option<&'static str>,
    services: Vec<ExternalService>,
    services_error: Option<&'static str>,
    listed: Vec<&'static str>,
    listed_error: Option<&'static str>,
    gitserver: Vec<(&'static str, CloneStatus)>,
    gitserver_error: Option<&'static str>,
    enqueue_errors: Vec<(&'static str, &'static str)>,
}

fn string_list(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

#[tokio::test]
async fn autoindexing_dependency_indexing_scheduler_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_DEPENDENCY_INDEXING_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }

    let base = OffsetDateTime::from_unix_timestamp(1_704_164_645).expect("base timestamp");

    let cases = vec![
        Case {
            name: "known-repos",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![
                gomod("https://github.com/sample/text", "v2.2.0"),
                gomod("https://github.com/sample/text", "v3.2.0"),
                gomod("https://github.com/cheese/burger", "v3.2.2"),
                gomod("https://github.com/cheese/burger", "v2.2.1"),
                gomod("https://github.com/cheese/burger", "v4.2.3"),
                gomod("https://github.com/sample/text", "v1.2.0"),
                gomod("https://github.com/banana/world", "v1.2.0"),
            ],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/cheese/burger", "github.com/sample/text"],
            listed_error: None,
            gitserver: vec![
                ("github.com/sample/text", CloneStatus::Cloned),
                ("github.com/cheese/burger", CloneStatus::Cloned),
            ],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "requeue-cloning",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![
                gomod("https://github.com/sample/text", "v3.2.0"),
                gomod("https://github.com/cheese/burger", "v4.2.3"),
            ],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/cheese/burger", "github.com/sample/text"],
            listed_error: None,
            gitserver: vec![
                ("github.com/sample/text", CloneStatus::Cloned),
                ("github.com/cheese/burger", CloneStatus::Cloning),
            ],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "skip-unknown-repo",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/banana/world", "v1.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![("github.com/banana/world", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "extsvc-no-services",
            auto_indexing: true,
            disabled: false,
            ext_kind: "JVMPACKAGES",
            ext_sync: Some(base),
            references: vec![reference("semanticdb", "banana", "v1.2.3")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![("banana", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "extsvc-up-to-date",
            auto_indexing: true,
            disabled: false,
            ext_kind: "JVMPACKAGES",
            ext_sync: Some(base),
            references: vec![reference("semanticdb", "banana", "v1.2.3")],
            references_error: None,
            services: vec![ExternalService {
                id: 7,
                last_sync_at: Some(base + std::time::Duration::from_secs(3600)),
            }],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![("banana", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "extsvc-outdated",
            auto_indexing: true,
            disabled: false,
            ext_kind: "JVMPACKAGES",
            ext_sync: Some(base),
            references: vec![reference("semanticdb", "banana", "v1.2.3")],
            references_error: None,
            services: vec![ExternalService {
                id: 7,
                last_sync_at: Some(base - std::time::Duration::from_secs(3600)),
            }],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![("banana", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "no-inferred-packages",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![reference("unknown-scheme", "whatever", "1.0.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "enqueue-error",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/sample/text"],
            listed_error: None,
            gitserver: vec![("github.com/sample/text", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![("gomod|https://github.com/sample/text|v2.2.0", "boom")],
        },
        Case {
            name: "multi-enqueue-error",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![
                gomod("https://github.com/sample/text", "v2.2.0"),
                gomod("https://github.com/cheese/burger", "v3.2.2"),
            ],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/cheese/burger", "github.com/sample/text"],
            listed_error: None,
            gitserver: vec![
                ("github.com/sample/text", CloneStatus::Cloned),
                ("github.com/cheese/burger", CloneStatus::Cloned),
            ],
            gitserver_error: None,
            enqueue_errors: vec![
                ("gomod|https://github.com/sample/text|v2.2.0", "boom-a"),
                ("gomod|https://github.com/cheese/burger|v3.2.2", "boom-b"),
            ],
        },
        Case {
            name: "autoindexing-disabled",
            auto_indexing: false,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "flag-disabled",
            auto_indexing: true,
            disabled: true,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "gitserver-missing",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/sample/text"],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "gitserver-not-cloned",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/sample/text"],
            listed_error: None,
            gitserver: vec![("github.com/sample/text", CloneStatus::NotCloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "duplicate-refs",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![
                gomod("https://github.com/sample/text", "v2.2.0"),
                gomod("https://github.com/sample/text", "v2.2.0"),
            ],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/sample/text"],
            listed_error: None,
            gitserver: vec![("github.com/sample/text", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "extsvc-list-error",
            auto_indexing: true,
            disabled: false,
            ext_kind: "JVMPACKAGES",
            ext_sync: None,
            references: vec![],
            references_error: None,
            services: vec![],
            services_error: Some("list-boom"),
            listed: vec![],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "references-error",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![],
            references_error: Some("refs-boom"),
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "repo-list-error",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec![],
            listed_error: Some("repos-boom"),
            gitserver: vec![("github.com/sample/text", CloneStatus::Cloned)],
            gitserver_error: None,
            enqueue_errors: vec![],
        },
        Case {
            name: "gitserver-error",
            auto_indexing: true,
            disabled: false,
            ext_kind: "",
            ext_sync: None,
            references: vec![gomod("https://github.com/sample/text", "v2.2.0")],
            references_error: None,
            services: vec![],
            services_error: None,
            listed: vec!["github.com/sample/text"],
            listed_error: None,
            gitserver: vec![],
            gitserver_error: Some("gitserver-boom"),
            enqueue_errors: vec![],
        },
    ];

    for case in cases {
        let uploads = std::sync::Arc::new(FakeUploads {
            references: case.references.clone(),
            error: case.references_error.map(str::to_owned),
            calls: Mutex::new(0),
        });
        let repos = std::sync::Arc::new(FakeRepos {
            listed: string_list(&case.listed),
            error: case.listed_error.map(str::to_owned),
            calls: Mutex::new(Vec::new()),
        });
        let external_services = std::sync::Arc::new(FakeExternalServices {
            services: case.services.clone(),
            error: case.services_error.map(str::to_owned),
            calls: Mutex::new(Vec::new()),
        });
        let gitserver = std::sync::Arc::new(FakeGitserver {
            repos: case
                .gitserver
                .iter()
                .map(|(name, status)| ((*name).to_owned(), status.clone()))
                .collect(),
            error: case.gitserver_error.map(str::to_owned),
            calls: Mutex::new(Vec::new()),
        });
        let enqueuer = std::sync::Arc::new(FakeEnqueuer {
            errors: case
                .enqueue_errors
                .iter()
                .map(|(key, error)| ((*key).to_owned(), (*error).to_owned()))
                .collect(),
            calls: Mutex::new(Vec::new()),
        });
        let worker_store = std::sync::Arc::new(FakeWorkerStore {
            requeued: Mutex::new(Vec::new()),
        });

        let handler = DependencyIndexingSchedulerHandler {
            uploads: uploads.clone(),
            repos: repos.clone(),
            external_services: external_services.clone(),
            gitserver_repos: gitserver.clone(),
            index_enqueuer: enqueuer.clone(),
            worker_store: worker_store.clone(),
            settings: HandlerSettings {
                auto_indexing_enabled: case.auto_indexing,
                disabled: case.disabled,
            },
        };

        let job = DependencyIndexingJob {
            id: 101,
            upload_id: 42,
            external_service_kind: case.ext_kind.to_owned(),
            external_service_sync: case.ext_sync,
        };

        let error = match handler.handle(&job).await {
            Ok(()) => "-".to_owned(),
            Err(HandleError::Message(message)) => message,
            Err(HandleError::Multiple(mut messages)) => {
                messages.sort();
                messages.join(" | ")
            }
        };

        let mut requeued = worker_store.requeued.lock().unwrap().clone();
        requeued.sort();

        let mut enqueued: Vec<String> = enqueuer
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(package_key)
            .collect();
        enqueued.sort();

        let mut ext_kinds: Vec<String> = external_services
            .calls
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect();
        ext_kinds.sort();

        let mut listed_names: Vec<String> = repos
            .calls
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect();
        listed_names.sort();

        let mut gitserver_names: Vec<String> = gitserver
            .calls
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect();
        gitserver_names.sort();

        emit(
            "dis",
            &DisLine {
                case: case.name,
                error,
                requeued,
                enqueued,
                ext_kinds,
                listed_names,
                gitserver_names,
                references_calls: *uploads.calls.lock().unwrap(),
            },
        );
    }

    // Keep the helper compiled even though the cases do not need an explicit
    // sorted subset.
    assert_eq!(
        set_difference(&["a".to_owned(), "b".to_owned()], &["a".to_owned()]),
        vec!["b".to_owned()]
    );
}
