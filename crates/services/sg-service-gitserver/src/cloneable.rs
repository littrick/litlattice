//! Port of `Server.IsRepoCloneable` (`cmd/gitserver/internal/server.go`) and
//! the parts of `vcssyncer.NewVCSSyncer` it reaches: the repository is looked
//! up, the on-disk clone state is reported, and the git remote is probed with
//! `ls-remote`.
//!
//! Only the git syncer is covered. Repository types the baseline routes to the
//! Perforce/package-manager syncers (which the Rust gitserver does not
//! implement) report `UNIMPLEMENTED`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sg_config::LiveConfig;
use sg_db::repos::RepoStore;
use tonic::Status;

use sg_proto::gitserver as gpb;

use crate::fs::repo_cloned;
use crate::grpc::is_always_cloning_test;
use crate::update::RemoteUrlSource;
use crate::vcssyncer;
use crate::vcsurl::VcsUrl;

/// `extsvc` service types that `NewVCSSyncer` maps to a non-git syncer. The
/// package-manager syncers are implemented; JVM/coursier is intentionally out
/// of scope and stays `UNIMPLEMENTED`.
const NON_GIT_TYPES: [&str; 7] = [
    "perforce",
    "jvmPackages",
    "npmPackages",
    "goModules",
    "pythonPackages",
    "rustPackages",
    "rubyPackages",
];

const PACKAGE_TYPES: [&str; 5] = [
    "npmPackages",
    "goModules",
    "pythonPackages",
    "rustPackages",
    "rubyPackages",
];

pub struct CloneableService {
    pub repos_dir: Arc<PathBuf>,
    /// `db.Repos()`: `NewVCSSyncer` reads the repository before anything else.
    pub repos: Arc<RepoStore>,
    /// The `getRemoteURLSource` closure's target: DB lookup, decryption and
    /// `cloneurl.ForEncryptableConfig`.
    pub remote_urls: Arc<dyn RemoteUrlSource>,
    pub live: Arc<LiveConfig>,
    /// `NewPerforceDepotSyncer`'s Perforce client, used by `IsCloneable`.
    pub perforce: Option<Arc<crate::perforce::PerforceService>>,
    /// `conf.Cached` for `executil.tlsExternal`, keyed by configuration
    /// generation so the CA bundle is written once per revision.
    tls_cache: Mutex<Option<(u64, crate::tlsexternal::TlsConfig)>>,
}

impl CloneableService {
    pub fn new(
        repos_dir: PathBuf,
        repos: Arc<RepoStore>,
        remote_urls: Arc<dyn RemoteUrlSource>,
        live: Arc<LiveConfig>,
    ) -> Self {
        Self {
            repos_dir: Arc::new(repos_dir),
            repos,
            remote_urls,
            live,
            perforce: None,
            tls_cache: Mutex::new(None),
        }
    }

    /// Wire the Perforce client used by `IsRepoCloneable` for Perforce repos.
    pub fn with_perforce(mut self, perforce: Arc<crate::perforce::PerforceService>) -> Self {
        self.perforce = Some(perforce);
        self
    }

    fn tls_config(&self) -> crate::tlsexternal::TlsConfig {
        let generation = self.live.generation();
        let mut cache = self.tls_cache.lock().unwrap_or_else(|err| err.into_inner());
        if let Some((cached_generation, tls)) = cache.as_ref() {
            if *cached_generation == generation {
                return tls.clone();
            }
        }
        let tls = crate::tlsexternal::TlsConfig::from_site(&self.live.current().site);
        *cache = Some((generation, tls.clone()));
        tls
    }

    /// `Server.IsRepoCloneable`. Errors from the repository lookup and the
    /// filesystem are RPC errors, exactly like the baseline; a remote that
    /// cannot be probed is reported in `reason` with `cloneable = false`.
    #[allow(clippy::result_large_err)] // `tonic::Status` is the RPC error type.
    pub async fn is_repo_cloneable(
        &self,
        repo: &str,
    ) -> Result<gpb::IsRepoCloneableResponse, Status> {
        // `getVCSSyncer` → `NewVCSSyncer` → `RepoStore.GetByName`.
        let stored = match self.repos.get_by_name(repo).await {
            Ok(Some(stored)) => stored,
            Ok(None) => {
                return Err(Status::unknown(format!(
                    "GetVCSSyncer: get repository: repo not found: name={repo:?}"
                )))
            }
            Err(err) => {
                return Err(Status::unknown(format!(
                    "GetVCSSyncer: get repository: {err}"
                )))
            }
        };
        if let Some(blocked) = &stored.blocked {
            let reason = blocked
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            return Err(Status::unknown(format!(
                "GetVCSSyncer: get repository: repository {} has been blocked. reason: {}",
                stored.name, reason
            )));
        }
        let service_type = stored.external_service_type.clone();
        if let Some(service_type) = service_type.as_deref() {
            if service_type == "jvmPackages" {
                return Err(Status::unimplemented(format!(
                    "GetVCSSyncer: {service_type} repositories are not implemented by the Rust gitserver yet"
                )));
            }
            if NON_GIT_TYPES.contains(&service_type)
                && !PACKAGE_TYPES.contains(&service_type)
                && service_type != "perforce"
            {
                return Err(Status::unimplemented(format!(
                    "GetVCSSyncer: {service_type} repositories are not implemented by the Rust gitserver yet"
                )));
            }
        }

        // `fs.RepoCloned`.
        let cloned = match repo_cloned(&self.repos_dir, repo) {
            Ok(cloned) => cloned,
            Err(err) => {
                return Err(Status::unknown(format!(
                    "determine if repo is cloned: {err}"
                )))
            }
        };

        // The instrumented syncer times the whole `IsCloneable` call,
        // including the remote URL lookup.
        let started = Instant::now();
        let result = match service_type.as_deref() {
            // `vcsPackagesSyncer.IsCloneable` always returns nil.
            Some(service_type) if PACKAGE_TYPES.contains(&service_type) => Ok(()),
            Some("perforce") => self.probe_perforce(repo).await,
            _ => self.probe(repo).await,
        };
        let cloneable = result.is_ok();
        crate::metrics::is_cloneable_observed(cloneable, started.elapsed().as_secs_f64());

        let reason = result.err().unwrap_or_default();
        Ok(gpb::IsRepoCloneableResponse {
            cloneable,
            cloned,
            reason,
        })
    }

    /// `gitRepoSyncer.IsCloneable`, with the baseline's error wrapping.
    async fn probe(&self, repo: &str) -> Result<(), String> {
        // `isAlwaysCloningTest`: tests treat the repo as always cloneable.
        if is_always_cloning_test(repo) {
            return Ok(());
        }

        let raw = self
            .remote_urls
            .remote_url(repo)
            .await
            .map_err(|err| format!("failed to get remote URL for {repo}: {}", err.message()))?;
        // `vcs.ParseURL` is total (it falls back to a local path).
        let remote = VcsUrl::parse(&raw)
            .map_err(|err| format!("failed to get remote URL for {repo}: {}", err.message()))?;
        let tls = self.tls_config();
        vcssyncer::check_cloneable(&remote, &tls)
            .await
            .map_err(|err| err.message().to_owned())
    }

    /// `perforceDepotSyncer.IsCloneable`, reusing the M9.15a Perforce client.
    async fn probe_perforce(&self, repo: &str) -> Result<(), String> {
        let Some(perforce) = self.perforce.as_deref() else {
            return Err("perforce syncer is not configured".to_owned());
        };
        let raw = self
            .remote_urls
            .remote_url(repo)
            .await
            .map_err(|err| format!("getting remote URL source: {}", err.message()))?;
        let remote = VcsUrl::parse(&raw)
            .map_err(|err| format!("getting remote URL: {}", err.message()))?;
        let (username, password, host, depot) =
            vcssyncer::decompose_perforce_remote_url(&remote)
                .map_err(|err| format!("invalid perforce remote URL: {err}"))?;
        perforce
            .is_depot_path_cloneable(
                &crate::perforce::ConnectionDetails {
                    p4port: host,
                    p4user: username,
                    p4passwd: password,
                },
                &depot,
            )
            .await
            .map_err(|err| err.message().to_owned())
    }
}
