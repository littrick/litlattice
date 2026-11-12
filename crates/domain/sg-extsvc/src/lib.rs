#![forbid(unsafe_code)]

//! `internal/extsvc` + `internal/repos/sources.go`: external services and the
//! repositories they yield.
//!
//! This slice implements the `OTHER` kind (`internal/repos/other.go`), which
//! is the only source that needs no code-host API. Other kinds plug into the
//! same [`Source`] trait; the syncer is source-agnostic.
//!
//! Sources take the plaintext configuration: the repo-updater source factory
//! decrypts `external_services.config` with the keyring built from site
//! configuration before calling [`new_source`], like `svc.Config.Decrypt(ctx)`
//! in the baseline.

pub mod auth;
pub mod awscodecommit;
pub mod batches_sources;
pub mod batches_state;
pub mod awscodecommit_source;
pub mod azuredevops;
pub mod azuredevops_source;
pub mod bitbucketcloud;
pub mod bitbucketcloud_source;
pub mod bitbucketserver;
pub mod bitbucketserver_source;
pub mod exclude;
pub mod gerrit;
pub mod gerrit_source;
pub mod github;
pub mod github_source;
pub mod gitlab;
pub mod gitlab_source;
pub mod gitolite;
pub mod gitolite_source;
pub mod go_modules;
pub mod go_packages;
pub mod jvm_packages;
pub mod npm_packages;
pub mod other;
pub mod packages;
pub mod pagure;
pub mod pagure_source;
pub mod perforce;
pub mod perforce_source;
pub mod phabricator;
pub mod phabricator_source;
pub mod python_packages;
pub mod ratelimit;
pub mod reposource;
pub mod reposource_packages;
pub mod ruby_packages;
pub mod rust_packages;

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// `extsvc.KindGitHub`.
pub const KIND_GITHUB: &str = "GITHUB";
/// `extsvc.KindGitLab`.
pub const KIND_GITLAB: &str = "GITLAB";
/// `extsvc.KindBitbucketCloud`.
pub const KIND_BITBUCKETCLOUD: &str = "BITBUCKETCLOUD";
/// `extsvc.KindAzureDevOps`.
pub const KIND_AZUREDEVOPS: &str = "AZUREDEVOPS";
/// `extsvc.KindGerrit`.
pub const KIND_GERRIT: &str = "GERRIT";
/// `extsvc.KindPerforce`.
pub const KIND_PERFORCE: &str = "PERFORCE";
/// `extsvc.KindAWSCodeCommit`.
pub const KIND_AWSCODECOMMIT: &str = "AWSCODECOMMIT";
/// `extsvc.KindBitbucketServer`.
pub const KIND_BITBUCKETSERVER: &str = "BITBUCKETSERVER";
/// `extsvc.KindGitolite`.
pub const KIND_GITOLITE: &str = "GITOLITE";
/// `extsvc.KindPagure`.
pub const KIND_PAGURE: &str = "PAGURE";
/// `extsvc.KindPhabricator`.
pub const KIND_PHABRICATOR: &str = "PHABRICATOR";
/// `extsvc.KindNpmPackages`.
pub const KIND_NPMPACKAGES: &str = "NPMPACKAGES";
/// `extsvc.KindPythonPackages`.
pub const KIND_PYTHONPACKAGES: &str = "PYTHONPACKAGES";
/// `extsvc.KindRubyPackages`.
pub const KIND_RUBYPACKAGES: &str = "RUBYPACKAGES";
/// `extsvc.KindRustPackages`.
pub const KIND_RUSTPACKAGES: &str = "RUSTPACKAGES";
/// `extsvc.KindGoPackages`.
pub const KIND_GOMODULES: &str = "GOMODULES";
/// `extsvc.KindJVMPackages`.
pub const KIND_JVMPACKAGES: &str = "JVMPACKAGES";
/// `extsvc.KindOther`.
pub const KIND_OTHER: &str = "OTHER";
/// `extsvc.TypeOther` (lowercase, stored in `repo.external_service_type`).
pub const TYPE_OTHER: &str = "other";

/// `extsvc.URN`: `extsvc:<lowercased kind>:<id>`.
pub fn urn(kind: &str, id: i64) -> String {
    format!("extsvc:{}:{}", kind.to_lowercase(), id)
}

/// `api.ExternalRepoSpec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRepoSpec {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "ServiceType")]
    pub service_type: String,
    #[serde(rename = "ServiceID")]
    pub service_id: String,
}

impl ExternalRepoSpec {
    /// `api.ExternalRepoSpec.Equal`.
    pub fn equal(&self, other: &Self) -> bool {
        !other.id.is_empty()
            && !other.service_id.is_empty()
            && !other.service_type.is_empty()
            && self == other
    }
}

/// `types.SourceInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceInfo {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "CloneURL")]
    pub clone_url: String,
}

/// The `types.Repo` fields the syncer owns. Values not produced by a source
/// stay at their zero value, exactly like the Go struct.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Repo {
    /// `types.Repo.ID`; zero for a repo that has not been stored yet.
    #[serde(default)]
    pub id: i32,
    /// `types.Repo.DeletedAt` reduced to `IsDeleted()`; only stored repos
    /// carry it (sources never set it).
    #[serde(default)]
    pub deleted: bool,
    pub name: String,
    pub uri: String,
    pub description: String,
    pub external_repo: ExternalRepoSpec,
    pub sources: BTreeMap<String, SourceInfo>,
    /// `repo.metadata` (`extsvc.OtherRepoMetadata` for this source).
    pub metadata: Option<serde_json::Value>,
    pub archived: bool,
    pub fork: bool,
    pub stars: i32,
    pub private: bool,
}

/// `types.RepoSyncDiff` plus the unmodified set, as produced by one sync.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RepoSyncDiff {
    pub added: Vec<Repo>,
    pub modified: Vec<ModifiedRepo>,
    pub deleted: Vec<Repo>,
    pub unmodified: Vec<Repo>,
}

impl RepoSyncDiff {
    pub fn len(&self) -> usize {
        self.added.len() + self.modified.len() + self.deleted.len() + self.unmodified.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `types.RepoSyncDiff.Repos`: every repo in the diff.
    pub fn repos(&self) -> Vec<&Repo> {
        self.added
            .iter()
            .chain(self.modified.iter().map(|m| &m.repo))
            .chain(self.deleted.iter())
            .chain(self.unmodified.iter())
            .collect()
    }
}

/// The subset of `Repo.Update`'s bitmask that decides whether a repo changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepoModifiedFields(u32);

impl RepoModifiedFields {
    pub const NAME: u32 = 1 << 0;
    pub const URI: u32 = 1 << 1;
    pub const DESCRIPTION: u32 = 1 << 2;
    pub const EXTERNAL_REPO: u32 = 1 << 3;
    pub const ARCHIVED: u32 = 1 << 4;
    pub const FORK: u32 = 1 << 5;
    pub const PRIVATE: u32 = 1 << 6;
    pub const STARS: u32 = 1 << 7;
    pub const METADATA: u32 = 1 << 8;
    pub const SOURCES: u32 = 1 << 9;

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn contains(&self, flag: u32) -> bool {
        self.0 & flag != 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModifiedRepo {
    pub repo: Repo,
    pub modified: RepoModifiedFields,
}

impl Repo {
    /// `types.Repo.Update`: copies the sourced values into the stored repo
    /// and reports which fields changed.
    pub fn update(&mut self, next: &Repo) -> RepoModifiedFields {
        let mut modified = 0;
        if self.name != next.name {
            self.name = next.name.clone();
            modified |= RepoModifiedFields::NAME;
        }
        if self.uri != next.uri {
            self.uri = next.uri.clone();
            modified |= RepoModifiedFields::URI;
        }
        if self.description != next.description {
            self.description = next.description.clone();
            modified |= RepoModifiedFields::DESCRIPTION;
        }
        if !next.external_repo.id.is_empty()
            && !next.external_repo.service_id.is_empty()
            && !next.external_repo.service_type.is_empty()
            && self.external_repo != next.external_repo
        {
            self.external_repo = next.external_repo.clone();
            modified |= RepoModifiedFields::EXTERNAL_REPO;
        }
        if self.archived != next.archived {
            self.archived = next.archived;
            modified |= RepoModifiedFields::ARCHIVED;
        }
        if self.fork != next.fork {
            self.fork = next.fork;
            modified |= RepoModifiedFields::FORK;
        }
        if self.private != next.private {
            self.private = next.private;
            modified |= RepoModifiedFields::PRIVATE;
        }
        if self.stars != next.stars {
            self.stars = next.stars;
            modified |= RepoModifiedFields::STARS;
        }
        if self.metadata != next.metadata {
            self.metadata = next.metadata.clone();
            modified |= RepoModifiedFields::METADATA;
        }
        for (urn, info) in &next.sources {
            if self.sources.get(urn) != Some(info) {
                self.sources.insert(urn.clone(), info.clone());
                modified |= RepoModifiedFields::SOURCES;
            }
        }
        RepoModifiedFields(modified)
    }
}

/// One external service as the syncer consumes it (`types.ExternalService`
/// with the config already decrypted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceService {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    pub config: String,
    pub encryption_key_id: String,
    pub unrestricted: bool,
}

impl SourceService {
    /// `types.ExternalService.URN`.
    pub fn urn(&self) -> String {
        urn(&self.kind, self.id)
    }
}

/// `repos.SourceResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceResult {
    pub repo: Option<Repo>,
    pub err: Option<String>,
}

impl SourceResult {
    pub fn repo(repo: Repo) -> Self {
        Self {
            repo: Some(repo),
            err: None,
        }
    }

    pub fn error(err: impl Into<String>) -> Self {
        Self {
            repo: None,
            err: Some(err.into()),
        }
    }
}

/// `httpcli.Doer`, reduced to the GET the src-expose path needs.
#[async_trait]
pub trait HttpGetter: Send + Sync {
    async fn get(&self, url: &str) -> Result<Vec<u8>, String>;
}

/// An outbound HTTP request once the client has built it. The GitHub client
/// only uses `GET` and `POST`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// An HTTP response with the status, header values in wire order and body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// `httpcli.Doer`: the injectable transport seam. Production uses
/// `sg-http::HttpClient`; tests and the differential harness inject fixed
/// responses.
#[async_trait]
pub trait HttpDoer: Send + Sync {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String>;
}

/// `repos.Source`.
#[async_trait]
pub trait Source: Send + Sync {
    /// `Source.ListRepos`. The baseline streams over a channel; this slice
    /// materialises the results because the sources are config-bounded.
    async fn list_repos(&self) -> Vec<SourceResult>;
    /// `Source.CheckConnection`; sources without a code-host API succeed
    /// without a request.
    async fn check_connection(&self) -> Result<(), String> {
        Ok(())
    }
    /// `Source.ExternalServices`.
    fn external_services(&self) -> Vec<SourceService>;
}

/// `repos.NewSource` for the kinds implemented so far.
///
/// `perforce` and `gitolite` are the gitserver RPC boundaries those sources
/// need. Production repo-updater supplies a gRPC-backed client for both;
/// without one, Perforce reports the explicit not-ported error and Gitolite
/// falls back to running `ssh <host> info` in-process. `github_apps` resolves
/// `gitHubAppDetails` to an installation token; without it such GitHub
/// connections are rejected by [`github_source::GitHubSource::new`].
pub async fn new_source(
    service: SourceService,
    http: Option<std::sync::Arc<dyn HttpGetter>>,
    doer: Option<std::sync::Arc<dyn HttpDoer>>,
    perforce: Option<std::sync::Arc<dyn perforce::PerforceGitserver>>,
    gitolite: Option<std::sync::Arc<dyn gitolite::GitoliteRepoLister>>,
    github_apps: Option<std::sync::Arc<dyn auth::GitHubAppStore>>,
) -> Result<Box<dyn Source>, String> {
    if service.kind.eq_ignore_ascii_case(KIND_OTHER) {
        Ok(Box::new(other::OtherSource::new(service, http)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_GITHUB) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(
            github_source::GitHubSource::new_with_github_apps(service, doer, github_apps).await?,
        ))
    } else if service.kind.eq_ignore_ascii_case(KIND_GITLAB) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(gitlab_source::GitLabSource::new(service, doer)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_AZUREDEVOPS) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(azuredevops_source::AzureDevOpsSource::new(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_GERRIT) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(gerrit_source::GerritSource::new(service, doer)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_BITBUCKETCLOUD) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(bitbucketcloud_source::BitbucketCloudSource::new(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_AWSCODECOMMIT) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(awscodecommit_source::AWSCodeCommitSource::new(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_BITBUCKETSERVER) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(bitbucketserver_source::BitbucketServerSource::new(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_GITOLITE) {
        // Production reaches gitserver's `ListGitolite`; without a client
        // (tests) fall back to the in-process `ssh <host> info` runner.
        let lister: std::sync::Arc<dyn gitolite::GitoliteRepoLister> = match gitolite {
            Some(lister) => lister,
            None => std::sync::Arc::new(gitolite::SshGitoliteCommand),
        };
        Ok(Box::new(gitolite_source::GitoliteSource::new(
            service, lister,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_PAGURE) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(pagure_source::PagureSource::new(service, doer)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_PHABRICATOR) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(phabricator_source::PhabricatorSource::new(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_NPMPACKAGES) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(packages::PackagesSource::new_npm(service, doer)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_PYTHONPACKAGES) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(packages::PackagesSource::new_python(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_RUBYPACKAGES) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(packages::PackagesSource::new_ruby(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_RUSTPACKAGES) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(packages::PackagesSource::new_rust(
            service, doer,
        )?))
    } else if service.kind.eq_ignore_ascii_case(KIND_GOMODULES) {
        let doer = doer.ok_or_else(|| {
            format!(
                "cannot create source for kind {:?}: no HTTP transport configured",
                service.kind
            )
        })?;
        Ok(Box::new(packages::PackagesSource::new_go(service, doer)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_JVMPACKAGES) {
        // `NewJVMPackagesSource` takes no client factory: the baseline only
        // uses coursier from the commented-out `Get`.
        Ok(Box::new(packages::PackagesSource::new_jvm(service)?))
    } else if service.kind.eq_ignore_ascii_case(KIND_PERFORCE) {
        // Perforce needs the gitserver, not an HTTP transport. Production
        // injects a gRPC client; without one (tests) the source reports the
        // explicit not-ported error when listing repos or checking.
        let source = match perforce {
            Some(gitserver) => {
                perforce_source::PerforceSource::new_with_gitserver(service, gitserver)?
            }
            None => perforce_source::PerforceSource::new(service)?,
        };
        Ok(Box::new(source))
    } else {
        Err(format!("cannot create source for kind {:?}", service.kind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urn_matches_the_baseline_format() {
        assert_eq!(urn("OTHER", 42), "extsvc:other:42");
    }

    #[test]
    fn update_reports_changed_fields() {
        let mut stored = Repo {
            name: "github.com/o/r".to_owned(),
            uri: "old".to_owned(),
            private: true,
            metadata: Some(serde_json::json!({"RelativePath": "/a"})),
            ..Repo::default()
        };
        let sourced = Repo {
            name: "github.com/o/r".to_owned(),
            uri: "new".to_owned(),
            private: true,
            metadata: Some(serde_json::json!({"RelativePath": "/a"})),
            ..Repo::default()
        };
        let modified = stored.update(&sourced);
        assert!(modified.contains(RepoModifiedFields::URI));
        assert!(!modified.contains(RepoModifiedFields::NAME));
        assert!(!modified.contains(RepoModifiedFields::METADATA));
        assert_eq!(stored.uri, "new");

        let unchanged = stored.update(&sourced);
        assert!(unchanged.is_empty());
    }
}
