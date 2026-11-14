#![forbid(unsafe_code)]

//! Client for `gitserver.v1.GitserverRepositoryService` (`FetchRepository`,
//! `DeleteRepository`).
//!
//! The Go baseline reaches gitserver through `internal/gitserver`:
//! `connection.AddrForRepo` shards on the repository name and
//! `atomicGitServerConns` re-reads the address list (and pinned repos) from
//! the live configuration, opening a connection per address. This module
//! mirrors that: addresses are read from [`sg_config::LiveConfig`] on every
//! call, and connections are cached per address. Same-repository requests
//! therefore keep hitting the same shard across configuration updates, and a
//! repository only moves when the baseline would move it too.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use prost_types::Timestamp;
use sg_config::LiveConfig;
use sg_git::addresses::GitserverAddresses;
use sg_metrics::prometheus::Gauge;
use sg_proto::gitserver as gpb;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};
use tonic::transport::Channel;

use crate::metrics::Metrics;
use crate::scheduler::RepoUpdateClient;

/// `SRC_GITSERVER_CLIENT_CONCURRENCY_LIMIT`'s default (`env.MustGetInt`).
const DEFAULT_CONCURRENCY_LIMIT: usize = 500;

fn endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}

/// `env.MustGetInt("SRC_GITSERVER_CLIENT_CONCURRENCY_LIMIT", 500, ...)`.
///
/// Erratum: the baseline panics on a malformed value; a drop-in server logs
/// and uses the default instead.
fn concurrency_limit() -> usize {
    let Ok(raw) = std::env::var("SRC_GITSERVER_CLIENT_CONCURRENCY_LIMIT") else {
        return DEFAULT_CONCURRENCY_LIMIT;
    };
    match raw.trim().parse::<usize>() {
        Ok(limit) if limit > 0 => limit,
        _ => {
            tracing::warn!(
                value = %raw,
                "invalid SRC_GITSERVER_CLIENT_CONCURRENCY_LIMIT; using 500"
            );
            DEFAULT_CONCURRENCY_LIMIT
        }
    }
}

/// `Timestamp.AsTime`.
pub fn timestamp_to_system_time(timestamp: Option<Timestamp>) -> std::time::SystemTime {
    use std::time::{Duration, UNIX_EPOCH};
    let Some(timestamp) = timestamp else {
        return UNIX_EPOCH;
    };
    let nanos = i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos);
    if nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanos as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos((-nanos) as u64)
    }
}

/// Inverse of [`timestamp_to_system_time`] (`timestamppb.New`).
pub fn system_time_to_timestamp(time: std::time::SystemTime) -> Timestamp {
    use std::time::UNIX_EPOCH;
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => Timestamp {
            seconds: after.as_secs() as i64,
            nanos: after.subsec_nanos() as i32,
        },
        Err(before) => {
            let duration = before.duration();
            let mut seconds = -(duration.as_secs() as i64);
            let mut nanos = -(duration.subsec_nanos() as i32);
            if nanos < 0 {
                seconds -= 1;
                nanos += 1_000_000_000;
            }
            Timestamp { seconds, nanos }
        }
    }
}

type Client = gpb::gitserver_repository_service_client::GitserverRepositoryServiceClient<Channel>;
type ServiceClient = gpb::gitserver_service_client::GitserverServiceClient<Channel>;

/// Keeps `src_gitserver_client_concurrent_requests` balanced with the
/// semaphore permit: the baseline's `observedLimiter` increments after
/// `Acquire` and decrements on `Release`.
struct ObservedPermit<'a> {
    _permit: SemaphorePermit<'a>,
    gauge: &'a Gauge,
}

impl Drop for ObservedPermit<'_> {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

pub struct GitserverRepoClient {
    live: Arc<LiveConfig>,
    /// One lazily-connected channel per gitserver address, shared by the
    /// repository and source-service clients (baseline `GitserverConns`).
    channels: Mutex<HashMap<String, Channel>>,
    limit: Semaphore,
    metrics: Arc<Metrics>,
}

impl GitserverRepoClient {
    pub fn new(live: Arc<LiveConfig>, metrics: Arc<Metrics>) -> Self {
        Self {
            live,
            channels: Mutex::new(HashMap::new()),
            limit: Semaphore::new(concurrency_limit()),
            metrics,
        }
    }

    /// `atomicGitServerConns.AddrForRepo`: read the shard list from the
    /// current configuration (so it follows `conf.Watch`), honor
    /// `experimentalFeatures.gitServerPinnedRepos`, and hash with MD5.
    fn address_for(&self, repo: &str) -> Option<String> {
        self.metrics.gitserver_addr_for_repo_invoked.inc();
        let unified = self.live.current();
        GitserverAddresses::from_unified(&unified)
            .addr_for_repo(repo)
            .map(str::to_owned)
    }

    /// `GitserverConns.ConnForRepo`: cached per-address channel. `Dial` is
    /// non-blocking in the baseline, so the channel is connected lazily and
    /// connection errors surface on the RPC, as they do in Go.
    // `tonic::Status` is the error type `RepoUpdateClient` uses; boxing it
    // here would only move the conversion inside each RPC.
    #[allow(clippy::result_large_err)]
    async fn channel_for(&self, addr: &str) -> Result<Channel, tonic::Status> {
        if let Some(channel) = self.channels.lock().await.get(addr) {
            return Ok(channel.clone());
        }
        let channel = Channel::from_shared(endpoint(addr))
            .map_err(|err| {
                tonic::Status::unavailable(format!("invalid gitserver address {addr:?}: {err}"))
            })?
            .connect_lazy();
        self.channels
            .lock()
            .await
            .insert(addr.to_owned(), channel.clone());
        Ok(channel)
    }

    #[allow(clippy::result_large_err)]
    async fn client_for(&self, addr: &str) -> Result<Client, tonic::Status> {
        Ok(Client::new(self.channel_for(addr).await?).max_decoding_message_size(128 * 1024 * 1024))
    }

    async fn service_client(&self, addr: &str) -> Result<ServiceClient, tonic::Status> {
        Ok(ServiceClient::new(self.channel_for(addr).await?)
            .max_decoding_message_size(128 * 1024 * 1024))
    }

    async fn observe(&self) -> ObservedPermit<'_> {
        let permit = self
            .limit
            .acquire()
            .await
            .expect("gitserver limiter is never closed");
        self.metrics.gitserver_client_concurrent_requests.inc();
        ObservedPermit {
            _permit: permit,
            gauge: &self.metrics.gitserver_client_concurrent_requests,
        }
    }

    #[allow(clippy::result_large_err)]
    async fn client_for_repo(&self, repo: &str) -> Result<Client, tonic::Status> {
        let Some(addr) = self.address_for(repo) else {
            return Err(tonic::Status::unavailable(
                "no gitserver endpoints configured",
            ));
        };
        self.client_for(&addr).await
    }

    /// Address selection errors are returned as plain text, like the
    /// baseline's `clientSource.ClientForRepo` errors; RPC errors keep their
    /// gRPC text (`rpc error: code = ... desc = ...`).
    async fn service_client_for_repo(&self, repo: &str) -> Result<ServiceClient, String> {
        let Some(addr) = self.address_for(repo) else {
            return Err("no gitserver endpoints configured".to_owned());
        };
        self.service_client(&addr)
            .await
            .map_err(|err| err.to_string())
    }
}

/// Go `status.Error(...).Error()` for a tonic status.
fn status_error_text(status: &tonic::Status) -> String {
    format!(
        "rpc error: code = {:?} desc = {}",
        status.code(),
        status.message()
    )
}

impl GitserverRepoClient {
    /// `repositoryServiceClient.FetchRepository`: the observation covers
    /// address selection and connection setup as well as the RPC, and errors
    /// from either count.
    #[allow(clippy::result_large_err)]
    pub async fn fetch_repository_with_scope(
        &self,
        repo: &str,
        scope: &str,
    ) -> Result<(std::time::SystemTime, std::time::SystemTime), tonic::Status> {
        let started = std::time::Instant::now();
        let result = self.fetch_repository_inner(repo).await;
        self.metrics.observe_gitserver_client(
            "FetchRepository",
            scope,
            started.elapsed().as_secs_f64(),
            result.is_err(),
        );
        result
    }

    #[allow(clippy::result_large_err)]
    async fn fetch_repository_inner(
        &self,
        repo: &str,
    ) -> Result<(std::time::SystemTime, std::time::SystemTime), tonic::Status> {
        let mut client = self.client_for_repo(repo).await?;
        let observed = self.observe().await;
        let response = client
            .fetch_repository(gpb::FetchRepositoryRequest {
                repo_name: repo.to_owned(),
            })
            .await?
            .into_inner();
        drop(observed);
        Ok((
            timestamp_to_system_time(response.last_fetched),
            timestamp_to_system_time(response.last_changed),
        ))
    }

    #[allow(clippy::result_large_err)]
    pub async fn delete_repository_with_scope(
        &self,
        repo: &str,
        scope: &str,
    ) -> Result<(), tonic::Status> {
        let started = std::time::Instant::now();
        let result = self.delete_repository_inner(repo).await;
        self.metrics.observe_gitserver_client(
            "DeleteRepository",
            scope,
            started.elapsed().as_secs_f64(),
            result.is_err(),
        );
        result
    }

    #[allow(clippy::result_large_err)]
    async fn delete_repository_inner(&self, repo: &str) -> Result<(), tonic::Status> {
        let mut client = self.client_for_repo(repo).await?;
        let observed = self.observe().await;
        client
            .delete_repository(gpb::DeleteRepositoryRequest {
                repo_name: repo.to_owned(),
            })
            .await?;
        drop(observed);
        Ok(())
    }

    /// `gitserver.Client.IsPerforcePathCloneable`, observed under `scope`
    /// (`repos.perforcesource`). `depotPath` is only the consistent-hashing
    /// key, like the baseline.
    pub async fn is_perforce_path_cloneable_with_scope(
        &self,
        connection: &sg_extsvc::perforce::PerforceConnectionDetails,
        depot: &str,
        scope: &str,
    ) -> Result<(), String> {
        let started = std::time::Instant::now();
        let result = self
            .is_perforce_path_cloneable_inner(connection, depot)
            .await;
        self.metrics.observe_gitserver_client(
            "IsPerforcePathCloneable",
            scope,
            started.elapsed().as_secs_f64(),
            result.is_err(),
        );
        result
    }

    async fn is_perforce_path_cloneable_inner(
        &self,
        connection: &sg_extsvc::perforce::PerforceConnectionDetails,
        depot: &str,
    ) -> Result<(), String> {
        let mut client = self.service_client_for_repo(depot).await?;
        let observed = self.observe().await;
        // The baseline unwraps proto errors for nicer messages.
        client
            .is_perforce_path_cloneable(gpb::IsPerforcePathCloneableRequest {
                connection_details: Some(perforce_connection_details(connection)),
                depot_path: depot.to_owned(),
            })
            .await
            .map_err(|err| err.message().to_owned())?;
        drop(observed);
        Ok(())
    }

    /// `gitserver.Client.CheckPerforceCredentials`; `p4port` is the hashing
    /// key, as in the baseline.
    pub async fn check_perforce_credentials_with_scope(
        &self,
        connection: &sg_extsvc::perforce::PerforceConnectionDetails,
        scope: &str,
    ) -> Result<(), String> {
        let started = std::time::Instant::now();
        let result = self.check_perforce_credentials_inner(connection).await;
        self.metrics.observe_gitserver_client(
            "CheckPerforceCredentials",
            scope,
            started.elapsed().as_secs_f64(),
            result.is_err(),
        );
        result
    }

    async fn check_perforce_credentials_inner(
        &self,
        connection: &sg_extsvc::perforce::PerforceConnectionDetails,
    ) -> Result<(), String> {
        let mut client = self
            .service_client_for_repo(&connection.p4_port)
            .await?;
        let observed = self.observe().await;
        client
            .check_perforce_credentials(gpb::CheckPerforceCredentialsRequest {
                connection_details: Some(perforce_connection_details(connection)),
            })
            .await
            .map_err(|err| err.message().to_owned())?;
        drop(observed);
        Ok(())
    }

    /// `gitserver.Client.ListGitoliteRepos`: the host is the hashing key; the
    /// baseline returns RPC errors unchanged (no message unwrapping).
    pub async fn list_gitolite_repos(
        &self,
        host: &str,
    ) -> Result<Vec<sg_extsvc::gitolite::Repo>, String> {
        let mut client = self.service_client_for_repo(host).await?;
        let observed = self.observe().await;
        let response = client
            .list_gitolite(gpb::ListGitoliteRequest {
                gitolite_host: host.to_owned(),
            })
            .await
            .map_err(|err| status_error_text(&err))?;
        drop(observed);
        Ok(response
            .into_inner()
            .repos
            .into_iter()
            .map(|repo| sg_extsvc::gitolite::Repo {
                name: repo.name,
                url: repo.url,
            })
            .collect())
    }
}

/// `protocol.PerforceConnectionDetails.ToProto`.
fn perforce_connection_details(
    details: &sg_extsvc::perforce::PerforceConnectionDetails,
) -> gpb::PerforceConnectionDetails {
    gpb::PerforceConnectionDetails {
        p4port: details.p4_port.clone(),
        p4user: details.p4_user.clone(),
        p4passwd: details.p4_passwd.clone(),
    }
}

/// `gitserver.NewRepositoryServiceClient(scope)`: the baseline builds one
/// wrapper per caller so the `scope` label says who issued the RPC
/// (`repoupdater.scheduler`, `repoupdater.purgeworker`,
/// `repoupdater.reclone`), while all wrappers share the connection cache.
pub struct ScopedGitserverClient {
    inner: Arc<GitserverRepoClient>,
    scope: &'static str,
}

impl ScopedGitserverClient {
    pub fn new(inner: Arc<GitserverRepoClient>, scope: &'static str) -> Self {
        Self { inner, scope }
    }
}

#[async_trait]
impl RepoUpdateClient for ScopedGitserverClient {
    async fn fetch_repository(
        &self,
        repo: &str,
    ) -> Result<(std::time::SystemTime, std::time::SystemTime), tonic::Status> {
        self.inner
            .fetch_repository_with_scope(repo, self.scope)
            .await
    }

    async fn delete_repository(&self, repo: &str) -> Result<(), tonic::Status> {
        self.inner
            .delete_repository_with_scope(repo, self.scope)
            .await
    }
}

/// `gitserver.Client` for the repo-updater code-host sources
/// (`PerforceSource`, `GitoliteSource`), backed by the same address selection
/// and channel cache as [`GitserverRepoClient`]. Scopes match the baseline
/// call sites (`repos.perforcesource`, `perforce.connectioncheck`,
/// `repos.gitolite`).
pub struct GitserverSourcesClient {
    inner: Arc<GitserverRepoClient>,
    perforce_list_scope: &'static str,
    perforce_check_scope: &'static str,
}

impl GitserverSourcesClient {
    /// Production wiring with the baseline's fixed scopes.
    pub fn production(inner: Arc<GitserverRepoClient>) -> Self {
        Self {
            inner,
            perforce_list_scope: "repos.perforcesource",
            perforce_check_scope: "perforce.connectioncheck",
        }
    }
}

#[async_trait]
impl sg_extsvc::perforce::PerforceGitserver for GitserverSourcesClient {
    async fn is_perforce_path_cloneable(
        &self,
        connection: &sg_extsvc::perforce::PerforceConnectionDetails,
        depot: &str,
    ) -> Result<(), String> {
        self.inner
            .is_perforce_path_cloneable_with_scope(connection, depot, self.perforce_list_scope)
            .await
    }

    async fn check_perforce_credentials(
        &self,
        connection: &sg_extsvc::perforce::PerforceConnectionDetails,
    ) -> Result<(), String> {
        self.inner
            .check_perforce_credentials_with_scope(connection, self.perforce_check_scope)
            .await
    }
}

#[async_trait]
impl sg_extsvc::gitolite::GitoliteRepoLister for GitserverSourcesClient {
    async fn list_repos(
        &self,
        host: &str,
    ) -> Result<Vec<sg_extsvc::gitolite::Repo>, String> {
        self.inner.list_gitolite_repos(host).await
    }
}

impl GitserverRepoClient {
    /// `gitserver.Client.CreateCommitFromPatch`: send the metadata event, then
    /// the patch as a client stream, and decode either the response or the
    /// typed `CreateCommitFromPatchError` carried in `grpc-status-details-bin`.
    pub async fn create_commit_from_patch_with_scope(
        &self,
        request: &sg_extsvc::batches_sources::CreateCommitFromPatchRequest,
        scope: &str,
    ) -> Result<
        sg_extsvc::batches_sources::CreateCommitFromPatchResponse,
        sg_extsvc::batches_sources::GitserverPushError,
    > {
        let started = std::time::Instant::now();
        let result = self.create_commit_from_patch_inner(request).await;
        self.metrics.observe_gitserver_client(
            "CreateCommitFromPatch",
            scope,
            started.elapsed().as_secs_f64(),
            result.is_err(),
        );
        result
    }

    async fn create_commit_from_patch_inner(
        &self,
        request: &sg_extsvc::batches_sources::CreateCommitFromPatchRequest,
    ) -> Result<
        sg_extsvc::batches_sources::CreateCommitFromPatchResponse,
        sg_extsvc::batches_sources::GitserverPushError,
    > {
        use sg_extsvc::batches_sources::{
            CreateCommitFromPatchResponse, GitserverPushError,
        };

        let Some(addr) = self.address_for(&request.repo) else {
            return Err(GitserverPushError::Other(
                "no gitserver endpoints configured".to_owned(),
            ));
        };
        let mut client = self
            .service_client(&addr)
            .await
            .map_err(|err| GitserverPushError::Other(err.to_string()))?;

        let metadata = gpb::create_commit_from_patch_binary_request::Metadata {
            repo: request.repo.clone(),
            base_commit: request.base_commit.clone(),
            target_ref: request.target_ref.clone(),
            commit_info: Some(gpb::PatchCommitInfo {
                messages: request.commit_info.messages.clone(),
                author_name: request.commit_info.author_name.clone(),
                author_email: request.commit_info.author_email.clone(),
                committer_name: String::new(),
                committer_email: String::new(),
                date: parse_rfc3339_timestamp(&request.commit_info.date),
            }),
            push: request.push.as_ref().map(|push| gpb::PushConfig {
                remote_url: push.remote_url.clone(),
                private_key: push.private_key.clone(),
                passphrase: push.passphrase.clone(),
            }),
            push_ref: None,
            patch_filenames_no_prefix: request.patch_filenames_no_prefix,
        };

        let mut events = Vec::with_capacity(2);
        events.push(gpb::CreateCommitFromPatchBinaryRequest {
            payload: Some(
                gpb::create_commit_from_patch_binary_request::Payload::Metadata(metadata),
            ),
        });
        if !request.patch.is_empty() {
            events.push(gpb::CreateCommitFromPatchBinaryRequest {
                payload: Some(
                    gpb::create_commit_from_patch_binary_request::Payload::Patch(
                        gpb::create_commit_from_patch_binary_request::Patch {
                            data: request.patch.clone(),
                        },
                    ),
                ),
            });
        }

        match client
            .create_commit_from_patch_binary(tokio_stream::iter(events))
            .await
        {
            Ok(response) => {
                let response = response.into_inner();
                Ok(CreateCommitFromPatchResponse {
                    rev: response.rev,
                    error: None,
                    changelist_id: response.changelist_id,
                })
            }
            Err(status) => Err(decode_create_commit_error(status)),
        }
    }
}

/// Decode `CreateCommitFromPatchError` from the `google.rpc.Status` payload
/// (`internal/gitserver/errwrap.go`); fall back to the status text.
fn decode_create_commit_error(
    status: tonic::Status,
) -> sg_extsvc::batches_sources::GitserverPushError {
    use prost::Message as _;
    use sg_extsvc::batches_sources::{CreateCommitFromPatchError, GitserverPushError};

    #[derive(Clone, PartialEq, prost::Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<prost_types::Any>,
    }

    if let Ok(rpc) = RpcStatus::decode(status.details()) {
        for any in &rpc.details {
            if any
                .type_url
                .ends_with("gitserver.v1.CreateCommitFromPatchError")
            {
                if let Ok(payload) =
                    gpb::CreateCommitFromPatchError::decode(any.value.as_slice())
                {
                    return GitserverPushError::Patch(CreateCommitFromPatchError {
                        repository_name: payload.repository_name,
                        internal_error: payload.internal_error,
                        command: payload.command,
                        combined_output: payload.combined_output,
                    });
                }
            }
        }
    }
    GitserverPushError::Other(status_error_text(&status))
}

/// `timestamppb.New` for the ISO-8601 `time.Time` JSON form used by
/// `PatchCommitInfo.Date`. Returns `None` for an empty/unparsable value, which
/// the baseline represents as the zero timestamp.
fn parse_rfc3339_timestamp(value: &str) -> Option<prost_types::Timestamp> {
    if value.is_empty() {
        return None;
    }
    let (date, rest) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let (time_part, offset_seconds) = if let Some(stripped) = rest.strip_suffix('Z') {
        (stripped, 0i64)
    } else if let Some(index) = rest.find(|ch| ch == '+' || ch == '-') {
        let (time_part, offset) = rest.split_at(index);
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let (hours, minutes) = offset.get(1..)?.split_once(':')?;
        let hours: i64 = hours.parse().ok()?;
        let minutes: i64 = minutes.parse().ok()?;
        (time_part, sign * (hours * 3600 + minutes * 60))
    } else {
        (rest, 0)
    };

    let mut time_parts = time_part.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let seconds_part = time_parts.next()?;
    let (seconds, fraction) = match seconds_part.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (seconds_part, ""),
    };
    let second: i64 = seconds.parse().ok()?;
    let nanos: i32 = if fraction.is_empty() {
        0
    } else {
        let mut digits = fraction.to_owned();
        digits.truncate(9);
        while digits.len() < 9 {
            digits.push('0');
        }
        digits.parse().ok()?
    };
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Days from `1970-01-01` (Howard Hinnant's `days_from_civil`).
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = if adjusted_year >= 0 { adjusted_year } else { adjusted_year - 399 } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month_prime = (month + 9) % 12;
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era =
        year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;

    let seconds_since_epoch =
        days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    Some(prost_types::Timestamp {
        seconds: seconds_since_epoch,
        nanos,
    })
}

#[async_trait]
impl sg_extsvc::batches_sources::GitserverCommitClient for GitserverSourcesClient {
    async fn create_commit_from_patch(
        &self,
        request: sg_extsvc::batches_sources::CreateCommitFromPatchRequest,
    ) -> Result<
        sg_extsvc::batches_sources::CreateCommitFromPatchResponse,
        sg_extsvc::batches_sources::GitserverPushError,
    > {
        self.inner
            .create_commit_from_patch_with_scope(&request, "batches.sources")
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_config::{RawUnified, ServiceConnections, Unified};

    fn unified(git_servers: &[&str], pinned: &[(&str, &str)]) -> Unified {
        let mut pins = serde_json::Map::new();
        for (repo, addr) in pinned {
            pins.insert(
                (*repo).to_owned(),
                serde_json::Value::String((*addr).to_owned()),
            );
        }
        let site = if pins.is_empty() {
            "{}".to_owned()
        } else {
            serde_json::json!({
                "experimentalFeatures": {"gitServerPinnedRepos": pins}
            })
            .to_string()
        };
        Unified::parse(&RawUnified {
            id: 0,
            site,
            service_connections: ServiceConnections {
                git_servers: git_servers.iter().map(|addr| (*addr).to_owned()).collect(),
                ..Default::default()
            },
        })
        .expect("parse config")
    }

    fn client(live: Arc<LiveConfig>) -> GitserverRepoClient {
        GitserverRepoClient::new(live, Arc::new(Metrics::new()))
    }

    #[test]
    fn shards_repositories_like_the_baseline() {
        let live = Arc::new(LiveConfig::new(unified(
            &["gitserver-1", "gitserver-2", "gitserver-3"],
            &[("repo2", "gitserver-1")],
        )));
        let client = client(live);
        // Golden values from `connection.TestAddrForRepo`.
        for (repo, want) in [
            ("repo1", "gitserver-3"),
            ("DELETED-123123.123123-repo1", "gitserver-3"),
            ("DELETED-123123.123123-repo2", "gitserver-1"),
            ("github.com/sourcegraph/sourcegraph", "gitserver-2"),
            ("github.com/sourcegraph/Sourcegraph", "gitserver-2"),
            ("repo2", "gitserver-1"),
        ] {
            assert_eq!(client.address_for(repo).as_deref(), Some(want), "{repo}");
        }
        assert_eq!(
            client.metrics.gitserver_addr_for_repo_invoked.get(),
            6.0,
            "AddrForRepo is counted once per call, as in the baseline"
        );
    }

    #[test]
    fn follows_the_live_configuration() {
        let live = Arc::new(LiveConfig::new(unified(&["gitserver-1"], &[])));
        let client = client(Arc::clone(&live));
        assert_eq!(client.address_for("repo1").as_deref(), Some("gitserver-1"));

        // The baseline's `conf.Watch` replaces the address list and pins
        // without restarting the process.
        live.set(unified(&["gitserver-1", "gitserver-2", "gitserver-3"], &[]));
        assert_eq!(client.address_for("repo1").as_deref(), Some("gitserver-3"));

        live.set(unified(
            &["gitserver-1", "gitserver-2", "gitserver-3"],
            &[("repo1", "gitserver-2")],
        ));
        assert_eq!(client.address_for("repo1").as_deref(), Some("gitserver-2"));
    }

    #[test]
    fn no_endpoints_is_unavailable_per_call() {
        let live = Arc::new(LiveConfig::new(unified(&[], &[])));
        let client = client(live);
        assert_eq!(client.address_for("repo1"), None);
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(client.client_for_repo("repo1"))
            .expect_err("no endpoints");
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn concurrency_limit_parsing() {
        // The variable is process-global; only assert the default path here
        // (other tests must not set it), and that the env override is read.
        assert_eq!(concurrency_limit(), DEFAULT_CONCURRENCY_LIMIT);
    }

    #[test]
    fn timestamps_round_trip() {
        use std::time::{Duration, UNIX_EPOCH};
        let now = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let timestamp = system_time_to_timestamp(now);
        assert_eq!(timestamp.seconds, 1_700_000_000);
        assert_eq!(timestamp.nanos, 123_456_789);
        assert_eq!(timestamp_to_system_time(Some(timestamp)), now);
    }

    #[test]
    fn missing_timestamp_is_unix_epoch() {
        assert_eq!(timestamp_to_system_time(None), std::time::UNIX_EPOCH);
    }
}
