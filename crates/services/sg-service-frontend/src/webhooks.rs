#![forbid(unsafe_code)]

//! Inbound repository webhooks:
//! `cmd/frontend/internal/repos/webhooks/handlers.go` and the push-event
//! parsing from `cmd/frontend/internal/webhooks`.
//!
//! A push webhook identifies a repository by its code-host external ID and
//! then asks repo-updater to re-sync the matching repository. Routing and
//! request classification stay in `external_api.rs` (M9.16c); this module only
//! parses the body and performs the sync through the two injected seams below,
//! so it is testable offline.

use async_trait::async_trait;
use serde_json::Value;

/// The code hosts that register a repository push handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeHostKind {
    GitHub,
    GitLab,
    BitbucketServer,
    BitbucketCloud,
}

impl CodeHostKind {
    /// `extsvc.Kind*`: the router's code-host key.
    pub fn kind(self) -> &'static str {
        match self {
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
            Self::BitbucketServer => sg_extsvc::bitbucketserver::TYPE_BITBUCKETSERVER,
            Self::BitbucketCloud => sg_extsvc::bitbucketcloud::TYPE_BITBUCKETCLOUD,
        }
    }

    /// `extsvc.Type*`: the value stored in `ExternalRepoSpec.ServiceType`.
    pub fn service_type(self) -> &'static str {
        match self {
            Self::GitHub => sg_extsvc::github::TYPE_GITHUB,
            Self::GitLab => sg_extsvc::gitlab::TYPE_GITLAB,
            Self::BitbucketServer => sg_extsvc::bitbucketserver::TYPE_BITBUCKETSERVER,
            Self::BitbucketCloud => sg_extsvc::bitbucketcloud::TYPE_BITBUCKETCLOUD,
        }
    }

    /// The event type each handler registers for.
    pub fn push_event_type(self) -> &'static str {
        match self {
            Self::GitHub | Self::GitLab => "push",
            Self::BitbucketServer => "repo:refs_changed",
            Self::BitbucketCloud => "repo:push",
        }
    }
}

/// `api.ExternalRepoSpec` fields used to look up the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRepoSpec {
    pub id: String,
    pub service_type: String,
    pub service_id: String,
}

/// The parsed push payload's repository identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushEvent {
    pub external_id: String,
}

/// The injectable database read (`db.Repos().List`).
#[async_trait]
pub trait WebhookRepoLister: Send + Sync {
    /// Return the repository names matching the external repo spec.
    async fn list_repos(&self, spec: &ExternalRepoSpec) -> Result<Vec<String>, String>;
}

/// The injectable repo-updater client (`repoupdater.DefaultClient.EnqueueRepoUpdate`).
#[async_trait]
pub trait RepoUpdateEnqueuer: Send + Sync {
    async fn enqueue_repo_update(&self, repo: &str) -> Result<String, String>;
}

/// Result of handling one push webhook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The event type has no push handler for this code host; ignored.
    NotAPushEvent,
    /// No repository matches the event's external ID.
    UnknownRepo,
    /// repo-updater accepted the update for this repository.
    Updated(String),
}

/// `ParseWebHook` for the four push payloads. Only the fields the handler
/// reads are decoded.
pub fn parse_push_event(kind: CodeHostKind, body: &[u8]) -> Result<PushEvent, String> {
    let value: Value = serde_json::from_slice(body).map_err(|err| err.to_string())?;
    if value.is_null() {
        return Err("incorrect event type: <nil>".to_owned());
    }
    match kind {
        CodeHostKind::GitHub => {
            let repository = value.get("repository");
            let id = repository
                .and_then(|repository| repository.get("id"))
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let node_id = repository
                .and_then(|repository| repository.get("node_id"))
                .and_then(Value::as_str);
            if id == 0 {
                return Err("external ID for repository not found".to_owned());
            }
            Ok(PushEvent {
                external_id: node_id.unwrap_or_default().to_owned(),
            })
        }
        CodeHostKind::GitLab => {
            let project_id = value
                .get("project_id")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            if project_id == 0 {
                return Err("project ID not found in PushEvent".to_owned());
            }
            Ok(PushEvent {
                external_id: project_id.to_string(),
            })
        }
        CodeHostKind::BitbucketServer => {
            let repository = value.get("repository");
            let id = repository
                .and_then(|repository| repository.get("id"))
                .and_then(Value::as_i64)
                .unwrap_or_default();
            if id == 0 {
                return Err("repository ID not found in PushEvent".to_owned());
            }
            Ok(PushEvent {
                external_id: id.to_string(),
            })
        }
        CodeHostKind::BitbucketCloud => Ok(PushEvent {
            external_id: value
                .get("repository")
                .and_then(|repository| repository.get("uuid"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }),
    }
}

/// `handlePushEvent`: parse the payload, find the repository by external ID
/// and enqueue a repo-updater sync for the first match.
pub async fn handle_push_event(
    kind: CodeHostKind,
    event_type: &str,
    base_url: &str,
    body: &[u8],
    repo_lister: &dyn WebhookRepoLister,
    enqueuer: &dyn RepoUpdateEnqueuer,
) -> Result<SyncOutcome, String> {
    if event_type != kind.push_event_type() {
        return Ok(SyncOutcome::NotAPushEvent);
    }
    let event = parse_push_event(kind, body)
        .map_err(|err| format!("getting external ID from event: {err}"))?;
    let spec = ExternalRepoSpec {
        id: event.external_id,
        service_type: kind.service_type().to_owned(),
        service_id: base_url.to_owned(),
    };
    let repos = repo_lister
        .list_repos(&spec)
        .await
        .map_err(|err| format!("handlePushEvent: ListRepos failed: {err}"))?;
    let Some(repo) = repos.first() else {
        return Ok(SyncOutcome::UnknownRepo);
    };
    let name = enqueuer
        .enqueue_repo_update(repo)
        .await
        .map_err(|err| format!("handlePushEvent: EnqueueRepoUpdate failed: {err}"))?;
    Ok(SyncOutcome::Updated(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_github_push_repository_ids() {
        let event = parse_push_event(
            CodeHostKind::GitHub,
            br#"{"repository":{"id":5,"node_id":"MDEwOlJlcG9zaXRvcnk1"}}"#,
        )
        .expect("github push");
        assert_eq!(event.external_id, "MDEwOlJlcG9zaXRvcnk1");
        assert_eq!(
            parse_push_event(CodeHostKind::GitHub, br#"{"repository":{}}"#).unwrap_err(),
            "external ID for repository not found"
        );
    }

    #[test]
    fn parses_gitlab_and_bitbucket_push_ids() {
        assert_eq!(
            parse_push_event(CodeHostKind::GitLab, br#"{"project_id":123}"#)
                .expect("gitlab")
                .external_id,
            "123"
        );
        assert_eq!(
            parse_push_event(
                CodeHostKind::BitbucketServer,
                br#"{"repository":{"id":99}}"#
            )
            .expect("bitbucket server")
            .external_id,
            "99"
        );
        assert_eq!(
            parse_push_event(
                CodeHostKind::BitbucketCloud,
                br#"{"repository":{"uuid":"{abc}"}}"#
            )
            .expect("bitbucket cloud")
            .external_id,
            "{abc}"
        );
    }

    struct FakeLister(Vec<String>);
    #[async_trait]
    impl WebhookRepoLister for FakeLister {
        async fn list_repos(&self, _spec: &ExternalRepoSpec) -> Result<Vec<String>, String> {
            Ok(self.0.clone())
        }
    }

    struct FakeEnqueuer;
    #[async_trait]
    impl RepoUpdateEnqueuer for FakeEnqueuer {
        async fn enqueue_repo_update(&self, repo: &str) -> Result<String, String> {
            Ok(repo.to_owned())
        }
    }

    #[tokio::test]
    async fn enqueues_the_first_matching_repo() {
        let lister = FakeLister(vec!["github.com/o/r".to_owned()]);
        let outcome = handle_push_event(
            CodeHostKind::GitHub,
            "push",
            "https://github.com/",
            br#"{"repository":{"id":1,"node_id":"R1"}}"#,
            &lister,
            &FakeEnqueuer,
        )
        .await
        .expect("handle");
        assert_eq!(
            outcome,
            SyncOutcome::Updated("github.com/o/r".to_owned())
        );

        let outcome = handle_push_event(
            CodeHostKind::GitHub,
            "repository",
            "https://github.com/",
            br#"{"repository":{"id":1,"node_id":"R1"}}"#,
            &FakeLister(Vec::new()),
            &FakeEnqueuer,
        )
        .await
        .expect("handle");
        assert_eq!(outcome, SyncOutcome::NotAPushEvent);

        let outcome = handle_push_event(
            CodeHostKind::GitLab,
            "push",
            "https://gitlab.com/",
            br#"{"project_id":1}"#,
            &FakeLister(Vec::new()),
            &FakeEnqueuer,
        )
        .await
        .expect("handle");
        assert_eq!(outcome, SyncOutcome::UnknownRepo);
    }
}
