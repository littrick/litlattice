#![forbid(unsafe_code)]

//! `internal/repos/packages.go` + the six `*_packages.go` sources: a
//! package-registry external service that yields one repository per dependency.
//!
//! The source enumerates the external service's configured `dependencies` and,
//! when a [`PackageRepoRefLister`] is installed, the package repositories the
//! dependency database reports for the source's scheme. `GetRepo` (the lazy
//! sync path) is exposed as an inherent method.
//!
//! # Boundaries
//!
//! The baseline reads the dependency database through
//! `dependencies.Service` (`ListPackageRepoRefs`/`IsPackageRepoAllowed`). The
//! Rust repo-updater has no equivalent service and M9.14a must not add tables,
//! so that access is an injectable [`PackageRepoRefLister`] seam. With no
//! lister installed the source behaves like the baseline with an empty
//! `package_repo_refs` table: only configured dependencies are yielded and
//! `GetRepo` allows the package. Pagination keeps the baseline's `After`/
//! `Limit=100` shape; the baseline's 32-way concurrency is replaced by a
//! sequential walk (same result set).
//!
//! `getPackageFromName` never calls the registry: the download-package branch
//! is commented out in the baseline (issue #39653). The registry clients are
//! therefore only reached through their own public methods, never by
//! enumeration.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::go_packages::GoModProxyClient;
use crate::npm_packages::NpmClient;
use crate::python_packages::PyPiClient;
use crate::reposource_packages::{
    parse_package_from_name, parse_package_from_repo_name, parse_versioned_package, PackageInfo,
    PackageKind,
};
use crate::ruby_packages::RubyGemsClient;
use crate::rust_packages::CratesClient;
use crate::{
    ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService,
};

/// One row from the dependency database, as far as the source needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoRef {
    pub id: i64,
    pub scheme: String,
    pub name: String,
}

/// The injectable seam over `dependencies.Service`'s two read methods.
///
/// `list_package_repo_refs` mirrors `ListPackageRepoRefs` with
/// `IncludeBlocked=false`: implementations return only unblocked rows ordered
/// by `id`, at most `limit` rows after `after`.
#[async_trait]
pub trait PackageRepoRefLister: Send + Sync {
    async fn list_package_repo_refs(
        &self,
        scheme: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<PackageRepoRef>, String>;

    async fn is_package_repo_allowed(&self, scheme: &str, name: &str) -> Result<bool, String>;
}

// ---------------------------------------------------------------------------
// Per-kind configuration (`schema.*PackagesConnection`)
// ---------------------------------------------------------------------------

/// `schema.NpmPackagesConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NpmPackagesConnection {
    #[serde(default)]
    pub registry: String,
    #[serde(default)]
    pub credentials: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

/// `schema.PythonPackagesConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PythonPackagesConnection {
    #[serde(default)]
    pub urls: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

/// `schema.RubyPackagesConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RubyPackagesConnection {
    #[serde(default)]
    pub repository: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

/// `schema.RustPackagesConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RustPackagesConnection {
    #[serde(default)]
    pub dependencies: Vec<String>,
}

/// `schema.GoModulesConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GoModulesConnection {
    #[serde(default)]
    pub urls: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

/// `schema.JVMPackagesConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct JvmPackagesConnection {
    #[serde(default)]
    pub maven: MavenConnection,
}

/// `schema.Maven`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MavenConnection {
    #[serde(default)]
    pub dependencies: Vec<String>,
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// The registry client each kind owns. Enumeration never calls it; it backs
/// the dependency-fetch methods (`FetchTarball`, `Download`, ...).
pub enum PackagesClient {
    Npm(NpmClient),
    Python(PyPiClient),
    Ruby(RubyGemsClient),
    Rust(CratesClient),
    Go(GoModProxyClient),
    Jvm,
}

impl std::fmt::Debug for PackagesClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Npm(_) => "npm",
            Self::Python(_) => "python",
            Self::Ruby(_) => "ruby",
            Self::Rust(_) => "rust",
            Self::Go(_) => "go",
            Self::Jvm => "jvm",
        };
        write!(f, "PackagesClient::{name}")
    }
}

/// `repos.PackagesSource`.
pub struct PackagesSource {
    service: SourceService,
    config_deps: Vec<String>,
    kind: PackageKind,
    lister: Option<Arc<dyn PackageRepoRefLister>>,
    client: PackagesClient,
}

impl std::fmt::Debug for PackagesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackagesSource")
            .field("kind", &self.kind)
            .field("config_deps", &self.config_deps)
            .field("has_lister", &self.lister.is_some())
            .finish()
    }
}

fn parse_config<T: for<'de> Deserialize<'de>>(service: &SourceService) -> Result<T, String> {
    let decoded = sg_config::strip_jsonc(&service.config);
    serde_json::from_str(&decoded)
        .map_err(|err| format!("external service id={} config error: {err}", service.id))
}

impl PackagesSource {
    fn new(
        service: SourceService,
        config_deps: Vec<String>,
        kind: PackageKind,
        client: PackagesClient,
    ) -> Self {
        Self {
            service,
            config_deps,
            kind,
            lister: None,
            client,
        }
    }

    /// `NewNpmPackagesSource`.
    pub fn new_npm(
        service: SourceService,
        doer: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let config: NpmPackagesConnection = parse_config(&service)?;
        let client = NpmClient::new(config.registry.clone(), config.credentials.clone(), doer);
        Ok(Self::new(
            service,
            config.dependencies,
            PackageKind::Npm,
            PackagesClient::Npm(client),
        ))
    }

    /// `NewPythonPackagesSource`.
    pub fn new_python(
        service: SourceService,
        doer: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let config: PythonPackagesConnection = parse_config(&service)?;
        let client = PyPiClient::new(config.urls.clone(), doer);
        Ok(Self::new(
            service,
            config.dependencies,
            PackageKind::Python,
            PackagesClient::Python(client),
        ))
    }

    /// `NewRubyPackagesSource`.
    pub fn new_ruby(
        service: SourceService,
        doer: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let config: RubyPackagesConnection = parse_config(&service)?;
        let client = RubyGemsClient::new(config.repository.clone(), doer);
        Ok(Self::new(
            service,
            config.dependencies,
            PackageKind::Ruby,
            PackagesClient::Ruby(client),
        ))
    }

    /// `NewRustPackagesSource`.
    pub fn new_rust(
        service: SourceService,
        doer: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let config: RustPackagesConnection = parse_config(&service)?;
        let client = CratesClient::new(doer);
        Ok(Self::new(
            service,
            config.dependencies,
            PackageKind::Rust,
            PackagesClient::Rust(client),
        ))
    }

    /// `NewGoPackagesSource`.
    pub fn new_go(
        service: SourceService,
        doer: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let config: GoModulesConnection = parse_config(&service)?;
        let client = GoModProxyClient::new(config.urls.clone(), doer);
        Ok(Self::new(
            service,
            config.dependencies,
            PackageKind::Go,
            PackagesClient::Go(client),
        ))
    }

    /// `NewJVMPackagesSource` (no client factory: the baseline uses coursier
    /// only from the commented-out `Get`).
    pub fn new_jvm(service: SourceService) -> Result<Self, String> {
        let config: JvmPackagesConnection = parse_config(&service)?;
        Ok(Self::new(
            service,
            config.maven.dependencies,
            PackageKind::Jvm,
            PackagesClient::Jvm,
        ))
    }

    /// `PackagesSource.SetDependenciesService`, reduced to the read seam.
    pub fn set_package_repo_ref_lister(
        &mut self,
        lister: Arc<dyn PackageRepoRefLister>,
    ) {
        self.lister = Some(lister);
    }

    pub fn with_package_repo_ref_lister(
        mut self,
        lister: Arc<dyn PackageRepoRefLister>,
    ) -> Self {
        self.lister = Some(lister);
        self
    }

    pub fn kind(&self) -> PackageKind {
        self.kind
    }

    pub fn scheme(&self) -> &'static str {
        self.kind.scheme()
    }

    pub fn client(&self) -> &PackagesClient {
        &self.client
    }

    /// `configDependencies`.
    fn config_dependencies(&self) -> Result<Vec<PackageInfo>, String> {
        let mut dependencies = Vec::with_capacity(self.config_deps.len());
        for dep in &self.config_deps {
            dependencies.push(parse_versioned_package(self.kind, dep)?);
        }
        Ok(dependencies)
    }

    /// `packageToRepoType`.
    pub fn package_to_repo(&self, dep: &PackageInfo) -> Repo {
        let urn = self.service.urn();
        let repo_name = dep.repo_name.clone();
        let service_type = self.kind.service_type().to_owned();

        let mut sources = BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn,
                clone_url: repo_name.clone(),
            },
        );

        Repo {
            name: repo_name.clone(),
            uri: repo_name.clone(),
            description: dep.description.clone(),
            external_repo: ExternalRepoSpec {
                id: repo_name.clone(),
                service_type: service_type.clone(),
                service_id: service_type,
            },
            sources,
            metadata: Some(dep.metadata.clone()),
            private: false,
            ..Repo::default()
        }
    }

    /// `PackagesSource.GetRepo`.
    pub async fn get_repo(&self, repo_name: &str) -> Result<Repo, String> {
        let parsed = parse_package_from_repo_name(self.kind, repo_name)?;

        if let Some(lister) = &self.lister {
            let allowed = lister
                .is_package_repo_allowed(self.kind.scheme(), &parsed.syntax)
                .await
                .map_err(|err| {
                    format!(
                        "error checking if package repo ({}, {}) is allowed: {err}",
                        self.kind.scheme(),
                        parsed.syntax
                    )
                })?;
            if !allowed {
                return Err(format!(
                    "repository not found (name={repo_name} notfound=true)"
                ));
            }
        }

        let pkg = get_package_from_name(self.kind, &parsed.syntax)?;
        Ok(self.package_to_repo(&pkg))
    }
}

/// `repos.getPackageFromName`: the download-package branch is commented out in
/// the baseline, so this is always `ParsePackageFromName`.
pub fn get_package_from_name(kind: PackageKind, name: &str) -> Result<PackageInfo, String> {
    parse_package_from_name(kind, name)
}

#[async_trait]
impl Source for PackagesSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut results = Vec::new();

        let static_config_deps = match self.config_dependencies() {
            Ok(dependencies) => dependencies,
            Err(err) => {
                results.push(SourceResult::error(err));
                return results;
            }
        };

        let mut handled_packages: BTreeSet<String> = BTreeSet::new();
        for dep in &static_config_deps {
            if handled_packages.contains(&dep.syntax) {
                continue;
            }
            if let Err(err) = get_package_from_name(self.kind, &dep.syntax) {
                results.push(SourceResult::error(err));
                continue;
            }
            results.push(SourceResult::repo(self.package_to_repo(dep)));
            handled_packages.insert(dep.syntax.clone());
        }

        let Some(lister) = &self.lister else {
            return results;
        };

        const BATCH_LIMIT: usize = 100;
        let mut last_id = 0i64;
        loop {
            let dep_repos = match lister
                .list_package_repo_refs(self.kind.scheme(), last_id, BATCH_LIMIT)
                .await
            {
                Ok(dep_repos) => dep_repos,
                Err(err) => {
                    results.push(SourceResult::error(err));
                    return results;
                }
            };
            if dep_repos.is_empty() {
                break;
            }
            last_id = dep_repos
                .last()
                .map(|repo| repo.id)
                .expect("non-empty batch");

            for dep_repo in dep_repos {
                if handled_packages.contains(&dep_repo.name) {
                    continue;
                }
                match get_package_from_name(self.kind, &dep_repo.name) {
                    Err(err) => results.push(SourceResult::error(err)),
                    Ok(pkg) => results.push(SourceResult::repo(self.package_to_repo(&pkg))),
                }
            }
        }

        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        // `PackagesSource.CheckConnection` always succeeds; failures surface
        // from the individual package/registry calls.
        Ok(())
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpResponse;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    struct NoRequests;

    #[async_trait]
    impl HttpDoer for NoRequests {
        async fn do_request(&self, _request: crate::HttpRequest) -> Result<HttpResponse, String> {
            panic!("package enumeration must not issue HTTP requests")
        }
    }

    fn service(kind: &str, config: &str) -> SourceService {
        SourceService {
            id: 9,
            kind: kind.to_owned(),
            display_name: kind.to_lowercase(),
            config: config.to_owned(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    fn name_and_uri(repo: &Repo) -> (String, String) {
        (repo.name.clone(), repo.uri.clone())
    }

    #[tokio::test]
    async fn npm_config_dependencies_become_repos_without_http() {
        let source = PackagesSource::new_npm(
            service(
                "NPMPACKAGES",
                r#"{"registry":"https://registry.npmjs.org","dependencies":["react@17.0.2","@types/lodash@4.14.177"]}"#,
            ),
            Arc::new(NoRequests),
        )
        .unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 2);
        let repos: Vec<_> = results.into_iter().filter_map(|result| result.repo).collect();
        assert_eq!(
            name_and_uri(&repos[0]),
            ("npm/react".to_owned(), "npm/react".to_owned())
        );
        assert_eq!(repos[0].external_repo.service_type, "npmPackages");
        assert_eq!(repos[0].external_repo.service_id, "npmPackages");
        assert_eq!(repos[0].external_repo.id, "npm/react");
        assert_eq!(
            repos[0].sources["extsvc:npmpackages:9"].clone_url,
            "npm/react"
        );
        assert_eq!(repos[0].metadata, Some(json!({"Package":{"Scope":"","Name":"react"}})));
        assert_eq!(repos[1].name, "npm/types/lodash");
        assert_eq!(
            repos[1].metadata,
            Some(json!({"Package":{"Scope":"types","Name":"lodash"}}))
        );
        assert_eq!(source.scheme(), "npm");
    }

    #[tokio::test]
    async fn each_kind_maps_its_config_dependencies() {
        let cases = vec![
            (
                "PYTHONPACKAGES",
                r#"{"urls":["https://pypi.org/simple"],"dependencies":["requests==2.1"]}"#,
                "python/requests",
                "pythonPackages",
                json!({}),
            ),
            (
                "RUBYPACKAGES",
                r#"{"repository":"https://rubygems.org/","dependencies":["rails@7.0"]}"#,
                "rubygems/rails",
                "rubyPackages",
                json!({}),
            ),
            (
                "RUSTPACKAGES",
                r#"{"dependencies":["serde@1.0"]}"#,
                "crates/serde",
                "rustPackages",
                json!({}),
            ),
            (
                "GOMODULES",
                r#"{"urls":["https://proxy.golang.org"],"dependencies":["github.com/gorilla/mux@v1.8.0"]}"#,
                "go/github.com/gorilla/mux",
                "goModules",
                json!({}),
            ),
            (
                "JVMPACKAGES",
                r#"{"maven":{"dependencies":["com.google.guava:guava:30.0-jre"]}}"#,
                "maven/com.google.guava/guava",
                "jvmPackages",
                json!({"Module":{"GroupID":"com.google.guava","ArtifactID":"guava"}}),
            ),
        ];

        for (kind, config, name, service_type, metadata) in cases {
            let source = match kind {
                "PYTHONPACKAGES" => PackagesSource::new_python(
                    service(kind, config),
                    Arc::new(NoRequests),
                )
                .unwrap(),
                "RUBYPACKAGES" => {
                    PackagesSource::new_ruby(service(kind, config), Arc::new(NoRequests)).unwrap()
                }
                "RUSTPACKAGES" => {
                    PackagesSource::new_rust(service(kind, config), Arc::new(NoRequests)).unwrap()
                }
                "GOMODULES" => {
                    PackagesSource::new_go(service(kind, config), Arc::new(NoRequests)).unwrap()
                }
                "JVMPACKAGES" => PackagesSource::new_jvm(service(kind, config)).unwrap(),
                other => panic!("unexpected kind {other}"),
            };
            let results = source.list_repos().await;
            assert_eq!(results.len(), 1, "{kind}");
            let repo = results[0].repo.as_ref().unwrap();
            assert_eq!(repo.name, name, "{kind}");
            assert_eq!(repo.uri, name, "{kind}");
            assert_eq!(repo.external_repo.service_type, service_type, "{kind}");
            assert_eq!(repo.metadata, Some(metadata), "{kind}");
        }
    }

    #[tokio::test]
    async fn invalid_config_dependencies_stop_enumeration() {
        let source = PackagesSource::new_npm(
            service(
                "NPMPACKAGES",
                r#"{"registry":"https://registry.npmjs.org","dependencies":["react@17.0.2","not-a-dependency"]}"#,
            ),
            Arc::new(NoRequests),
        )
        .unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].repo.is_none());
        assert_eq!(
            results[0].err.as_deref(),
            Some("expected dependency in (@scope/)?name@version format but found not-a-dependency")
        );
    }

    #[test]
    fn parse_config_errors_are_wrapped_like_the_baseline() {
        let err = PackagesSource::new_jvm(service("JVMPACKAGES", "{")).unwrap_err();
        assert!(
            err.starts_with("external service id=9 config error: "),
            "{err}"
        );
    }

    struct FakeLister {
        rows: Mutex<Vec<PackageRepoRef>>,
        allowed: Mutex<BTreeMap<String, bool>>,
    }

    #[async_trait]
    impl PackageRepoRefLister for FakeLister {
        async fn list_package_repo_refs(
            &self,
            scheme: &str,
            after: i64,
            limit: usize,
        ) -> Result<Vec<PackageRepoRef>, String> {
            let rows = self.rows.lock().unwrap();
            Ok(rows
                .iter()
                .filter(|row| row.scheme == scheme && row.id > after)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn is_package_repo_allowed(
            &self,
            _scheme: &str,
            name: &str,
        ) -> Result<bool, String> {
            Ok(*self.allowed.lock().unwrap().get(name).unwrap_or(&true))
        }
    }

    fn lister(rows: Vec<PackageRepoRef>) -> Arc<FakeLister> {
        Arc::new(FakeLister {
            rows: Mutex::new(rows),
            allowed: Mutex::new(BTreeMap::new()),
        })
    }

    #[tokio::test]
    async fn lister_rows_are_deduped_and_paginated() {
        let mut rows: Vec<PackageRepoRef> = (1..=149)
            .map(|id| PackageRepoRef {
                id,
                scheme: "npm".to_owned(),
                name: format!("pkg{id}"),
            })
            .collect();
        rows.push(PackageRepoRef {
            id: 150,
            scheme: "npm".to_owned(),
            name: "react".to_owned(),
        });
        let source = PackagesSource::new_npm(
            service(
                "NPMPACKAGES",
                r#"{"registry":"https://registry.npmjs.org","dependencies":["react@1.0.0"]}"#,
            ),
            Arc::new(NoRequests),
        )
        .unwrap()
        .with_package_repo_ref_lister(lister(rows));

        let results = source.list_repos().await;
        // 149 DB rows across two pages (100 + 49) + the configured react.
        // The DB row for react (id 150) is skipped because it was handled.
        assert_eq!(results.len(), 150);
        assert!(results.iter().all(|result| result.err.is_none()));
        let names: Vec<String> = results
            .iter()
            .filter_map(|result| result.repo.as_ref().map(|repo| repo.name.clone()))
            .collect();
        assert_eq!(names[0], "npm/react");
        assert!(names.contains(&"npm/pkg1".to_owned()));
        assert!(names.contains(&"npm/pkg149".to_owned()));
        assert_eq!(
            names.iter().filter(|name| name.as_str() == "npm/react").count(),
            1
        );
    }

    #[tokio::test]
    async fn get_repo_checks_the_allow_list() {
        let lister = lister(Vec::new());
        lister
            .allowed
            .lock()
            .unwrap()
            .insert("blocked".to_owned(), false);
        let source = PackagesSource::new_npm(
            service(
                "NPMPACKAGES",
                r#"{"registry":"https://registry.npmjs.org"}"#,
            ),
            Arc::new(NoRequests),
        )
        .unwrap()
        .with_package_repo_ref_lister(lister);

        let repo = source.get_repo("npm/react").await.unwrap();
        assert_eq!(repo.name, "npm/react");
        assert_eq!(
            source.get_repo("npm/blocked").await.unwrap_err(),
            "repository not found (name=npm/blocked notfound=true)"
        );
        assert_eq!(
            source.get_repo("python/requests").await.unwrap_err(),
            "expected path in npm/(scope/)?name format but found python/requests"
        );
    }

    #[tokio::test]
    async fn check_connection_is_always_ok() {
        let source = PackagesSource::new_jvm(service("JVMPACKAGES", "{}")).unwrap();
        assert!(source.check_connection().await.is_ok());
        assert_eq!(source.external_services().len(), 1);
    }
}
