#![forbid(unsafe_code)]

//! Client for `gitserver.v1.GitserverRepositoryService` (`FetchRepository`,
//! `DeleteRepository`).
//!
//! The Go baseline reaches gitserver through the load-balanced endpoint map
//! (`internal/gitserver`). The Rust pilot uses round-robin over the addresses
//! the frontend advertises in `ServiceConnections.GitServers` and refreshes
//! them only at startup (documented divergence; the Go client watches
//! configuration).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use prost_types::Timestamp;
use sg_proto::gitserver as gpb;
use tonic::transport::Channel;

use crate::scheduler::RepoUpdateClient;

fn endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}

/// `Timestamp.AsTime`.
pub fn timestamp_to_system_time(timestamp: Option<Timestamp>) -> SystemTime {
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
pub fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
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

pub struct GitserverRepoClient {
    clients: Vec<Client>,
    next: AtomicUsize,
}

impl GitserverRepoClient {
    pub fn connect(addrs: &[String]) -> Result<Self, String> {
        let mut clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let channel = Channel::from_shared(endpoint(addr))
                .map_err(|err| format!("invalid gitserver address {addr:?}: {err}"))?
                .connect_lazy();
            clients.push(Client::new(channel).max_decoding_message_size(128 * 1024 * 1024));
        }
        // An empty endpoint list is not a startup error in the baseline: the
        // client is built lazily and every request fails until gitserver
        // endpoints are known. Report `Unavailable` per call instead.
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    fn client(&self) -> Option<Client> {
        if self.clients.is_empty() {
            return None;
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        Some(self.clients[index].clone())
    }
}

#[async_trait]
impl RepoUpdateClient for GitserverRepoClient {
    async fn fetch_repository(
        &self,
        repo: &str,
    ) -> Result<(SystemTime, SystemTime), tonic::Status> {
        let Some(mut client) = self.client() else {
            return Err(tonic::Status::unavailable(
                "no gitserver endpoints configured",
            ));
        };
        let response = client
            .fetch_repository(gpb::FetchRepositoryRequest {
                repo_name: repo.to_owned(),
            })
            .await?
            .into_inner();
        Ok((
            timestamp_to_system_time(response.last_fetched),
            timestamp_to_system_time(response.last_changed),
        ))
    }

    async fn delete_repository(&self, repo: &str) -> Result<(), tonic::Status> {
        let Some(mut client) = self.client() else {
            return Err(tonic::Status::unavailable(
                "no gitserver endpoints configured",
            ));
        };
        client
            .delete_repository(gpb::DeleteRepositoryRequest {
                repo_name: repo.to_owned(),
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_round_trip() {
        let now = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let timestamp = system_time_to_timestamp(now);
        assert_eq!(timestamp.seconds, 1_700_000_000);
        assert_eq!(timestamp.nanos, 123_456_789);
        assert_eq!(timestamp_to_system_time(Some(timestamp)), now);
    }

    #[test]
    fn missing_timestamp_is_unix_epoch() {
        assert_eq!(timestamp_to_system_time(None), UNIX_EPOCH);
    }

    #[test]
    fn empty_endpoint_list_is_not_a_connect_error() {
        assert!(GitserverRepoClient::connect(&[]).is_ok());
    }
}
