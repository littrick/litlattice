#![forbid(unsafe_code)]

//! `cmd/repo-updater/internal/repoupdater/server.go`: the four
//! `repoupdater.v1.RepoUpdaterService` RPCs.

use std::sync::Arc;

use async_trait::async_trait;
use sg_proto::repoupdater as pb;
use tonic::{Request, Response, Status};

use crate::gitserver::system_time_to_timestamp;
use crate::scheduler::{RepoUpdateClient, SchedulerInfo, UpdateScheduler};
use crate::store::RepoNameStore;

/// `batches/syncer.ChangesetSyncRegistry`, reduced to the enqueue entry point.
/// The baseline only constructs one when Batch Changes is enabled; without it
/// `EnqueueChangesetSync` reports `changeset syncer is not configured`.
#[async_trait]
pub trait ChangesetSyncEnqueuer: Send + Sync {
    async fn enqueue_changeset_syncs(&self, ids: Vec<i64>) -> Result<(), String>;
}

pub struct RepoUpdaterGrpc {
    pub scheduler: Arc<UpdateScheduler>,
    pub repos: Arc<dyn RepoNameStore>,
    pub gitserver: Arc<dyn RepoUpdateClient>,
    pub changeset_sync: Option<Arc<dyn ChangesetSyncEnqueuer>>,
}

/// Go's `fmt.Sprintf("%q", name)` for the names the RPC receives.
pub fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for c in value.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            c if c.is_control() => quoted.push_str(&format!("\\x{:02x}", c as u32)),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

fn schedule_to_proto(state: crate::scheduler::schedule::ScheduleState) -> pb::RepoScheduleState {
    pb::RepoScheduleState {
        index: state.index as i64,
        total: state.total as i64,
        interval_seconds: state.interval_seconds,
        due: Some(system_time_to_timestamp(state.due)),
    }
}

fn queue_to_proto(state: crate::scheduler::updatequeue::QueueState) -> pb::RepoQueueState {
    pb::RepoQueueState {
        index: state.index as i64,
        total: state.total as i64,
        updating: state.updating,
        priority: i64::from(state.priority),
    }
}

pub fn schedule_info_to_proto(info: SchedulerInfo) -> pb::RepoUpdateSchedulerInfoResponse {
    pb::RepoUpdateSchedulerInfoResponse {
        schedule: info.schedule.map(schedule_to_proto),
        queue: info.queue.map(queue_to_proto),
    }
}

#[tonic::async_trait]
impl pb::repo_updater_service_server::RepoUpdaterService for RepoUpdaterGrpc {
    async fn repo_update_scheduler_info(
        &self,
        request: Request<pb::RepoUpdateSchedulerInfoRequest>,
    ) -> Result<Response<pb::RepoUpdateSchedulerInfoResponse>, Status> {
        let id = request.into_inner().id;
        Ok(Response::new(schedule_info_to_proto(
            self.scheduler.schedule_info(id),
        )))
    }

    async fn enqueue_repo_update(
        &self,
        request: Request<pb::EnqueueRepoUpdateRequest>,
    ) -> Result<Response<pb::EnqueueRepoUpdateResponse>, Status> {
        let repo_name = request.into_inner().repo;
        let repos = self
            .repos
            .list_by_names(std::slice::from_ref(&repo_name))
            .await
            .map_err(|err| Status::unknown(format!("store.list-repos: {err}")))?;
        if repos.len() != 1 {
            return Err(Status::not_found(format!(
                "repo {} not found in store",
                go_quote(&repo_name)
            )));
        }
        let repo = &repos[0];
        self.scheduler.update_once(repo.id, repo.name.clone());
        Ok(Response::new(pb::EnqueueRepoUpdateResponse {
            id: repo.id,
            name: repo.name.clone(),
        }))
    }

    async fn reclone_repository(
        &self,
        request: Request<pb::RecloneRepositoryRequest>,
    ) -> Result<Response<pb::RecloneRepositoryResponse>, Status> {
        let repo_name = request.into_inner().repo_name;
        if repo_name.is_empty() {
            return Err(Status::invalid_argument("repo_name must be specified"));
        }

        let repos = self
            .repos
            .list_by_names(std::slice::from_ref(&repo_name))
            .await
            .map_err(|err| Status::unknown(format!("store.list-repos: {err}")))?;
        if repos.len() != 1 {
            return Err(Status::not_found(format!(
                "repo {} not found in store",
                go_quote(&repo_name)
            )));
        }
        let repo = &repos[0];

        if let Err(err) = self.gitserver.delete_repository(&repo_name).await {
            return Err(Status::internal(format!(
                "failed to delete repository {}: {err}",
                go_quote(&repo_name)
            )));
        }

        self.scheduler.update_once(repo.id, repo.name.clone());
        Ok(Response::new(pb::RecloneRepositoryResponse {}))
    }

    async fn enqueue_changeset_sync(
        &self,
        request: Request<pb::EnqueueChangesetSyncRequest>,
    ) -> Result<Response<pb::EnqueueChangesetSyncResponse>, Status> {
        let ids = request.into_inner().ids;
        let Some(registry) = self.changeset_sync.as_ref() else {
            tracing::warn!("ChangesetSyncer is nil");
            return Err(Status::internal("changeset syncer is not configured"));
        };
        if ids.is_empty() {
            return Err(Status::invalid_argument("no ids provided"));
        }
        registry
            .enqueue_changeset_syncs(ids)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(pb::EnqueueChangesetSyncResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use sg_config::{LiveConfig, RawUnified, Unified};

    use super::*;
    use crate::metrics::Metrics;
    use crate::scheduler::{Clock, SchedulerStore};
    use crate::store::RepoName;

    struct FakeStore;

    #[async_trait]
    impl RepoNameStore for FakeStore {
        async fn list_by_names(&self, names: &[String]) -> Result<Vec<RepoName>, String> {
            Ok(names
                .iter()
                .filter(|name| name.as_str() == "github.com/example/repo")
                .map(|name| RepoName {
                    id: 42,
                    name: name.clone(),
                })
                .collect())
        }
    }

    struct FakeSchedulerStore;

    #[async_trait]
    impl SchedulerStore for FakeSchedulerStore {
        async fn iterate_repo_gitserver_status(
            &self,
            _cursor: i32,
            _batch_size: i64,
        ) -> Result<(Vec<crate::scheduler::RepoStatus>, i32), String> {
            Ok((Vec::new(), 0))
        }

        async fn set_last_fetched(
            &self,
            _repo: &str,
            _last_fetched: SystemTime,
            _last_changed: SystemTime,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            UNIX_EPOCH
        }
    }

    struct FakeGitserver {
        fail_delete: bool,
    }

    #[async_trait]
    impl RepoUpdateClient for FakeGitserver {
        async fn fetch_repository(
            &self,
            _repo: &str,
        ) -> Result<(SystemTime, SystemTime), tonic::Status> {
            Ok((UNIX_EPOCH, UNIX_EPOCH))
        }

        async fn delete_repository(&self, repo: &str) -> Result<(), tonic::Status> {
            if self.fail_delete {
                return Err(tonic::Status::unavailable("gitserver unavailable"));
            }
            let _ = repo;
            Ok(())
        }
    }

    fn server(fail_delete: bool) -> (RepoUpdaterGrpc, Arc<UpdateScheduler>) {
        let live = Arc::new(LiveConfig::new(
            Unified::parse(&RawUnified {
                site: "{}".to_owned(),
                ..Default::default()
            })
            .expect("site config"),
        ));
        let scheduler = Arc::new(UpdateScheduler::new(
            Arc::new(FixedClock),
            Arc::new(FakeSchedulerStore),
            Arc::new(FakeGitserver { fail_delete }),
            live,
            Arc::new(Metrics::new()),
        ));
        (
            RepoUpdaterGrpc {
                scheduler: Arc::clone(&scheduler),
                repos: Arc::new(FakeStore),
                gitserver: Arc::new(FakeGitserver { fail_delete }),
                changeset_sync: None,
            },
            scheduler,
        )
    }

    #[tokio::test]
    async fn scheduler_info_of_unknown_repo_is_empty() {
        let (server, _scheduler) = server(false);
        let response =
            pb::repo_updater_service_server::RepoUpdaterService::repo_update_scheduler_info(
                &server,
                Request::new(pb::RepoUpdateSchedulerInfoRequest { id: 1 }),
            )
            .await
            .expect("response")
            .into_inner();
        assert!(response.schedule.is_none());
        assert!(response.queue.is_none());
    }

    #[tokio::test]
    async fn enqueue_unknown_repo_is_not_found() {
        let (server, _scheduler) = server(false);
        let status = pb::repo_updater_service_server::RepoUpdaterService::enqueue_repo_update(
            &server,
            Request::new(pb::EnqueueRepoUpdateRequest {
                repo: "github.com/example/missing".to_owned(),
            }),
        )
        .await
        .expect_err("not found");
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(
            status.message(),
            "repo \"github.com/example/missing\" not found in store"
        );
    }

    #[tokio::test]
    async fn enqueue_known_repo_returns_identity_and_queues_it() {
        let (server, scheduler) = server(false);
        let response = pb::repo_updater_service_server::RepoUpdaterService::enqueue_repo_update(
            &server,
            Request::new(pb::EnqueueRepoUpdateRequest {
                repo: "github.com/example/repo".to_owned(),
            }),
        )
        .await
        .expect("response")
        .into_inner();
        assert_eq!(response.id, 42);
        assert_eq!(response.name, "github.com/example/repo");
        let info = scheduler.schedule_info(42);
        assert!(info.schedule.is_none());
        assert_eq!(info.queue.expect("queued").priority, 1);
    }

    #[tokio::test]
    async fn reclone_requires_a_repo_name() {
        let (server, _scheduler) = server(false);
        let status = pb::repo_updater_service_server::RepoUpdaterService::reclone_repository(
            &server,
            Request::new(pb::RecloneRepositoryRequest::default()),
        )
        .await
        .expect_err("invalid argument");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "repo_name must be specified");
    }

    #[tokio::test]
    async fn reclone_unknown_repo_is_not_found() {
        let (server, _scheduler) = server(false);
        let status = pb::repo_updater_service_server::RepoUpdaterService::reclone_repository(
            &server,
            Request::new(pb::RecloneRepositoryRequest {
                repo_name: "github.com/example/missing".to_owned(),
            }),
        )
        .await
        .expect_err("not found");
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn reclone_delete_failure_is_internal() {
        let (server, _scheduler) = server(true);
        let status = pb::repo_updater_service_server::RepoUpdaterService::reclone_repository(
            &server,
            Request::new(pb::RecloneRepositoryRequest {
                repo_name: "github.com/example/repo".to_owned(),
            }),
        )
        .await
        .expect_err("internal");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status
            .message()
            .starts_with("failed to delete repository \"github.com/example/repo\": "));
    }

    #[tokio::test]
    async fn changeset_sync_without_registry_is_not_configured() {
        let (server, _scheduler) = server(false);
        let status = pb::repo_updater_service_server::RepoUpdaterService::enqueue_changeset_sync(
            &server,
            Request::new(pb::EnqueueChangesetSyncRequest { ids: vec![1] }),
        )
        .await
        .expect_err("internal");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "changeset syncer is not configured");
    }
}
