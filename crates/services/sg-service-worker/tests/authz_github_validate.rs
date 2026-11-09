#![forbid(unsafe_code)]

//! M9.8bc differential harness (Rust side): `Provider.FetchAccount` and
//! `Provider.ValidateConnection` (required OAuth scopes / groups-cache gating).
//!
//! Mirrors `internal/authz/providers/github/authz_validate_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_authz_github_validate_diff.sh`.

use std::sync::Mutex;

use async_trait::async_trait;
use sg_db::users::UserRecord;
use sg_service_worker::jobs::authz_github::{
    Collaborator, GithubClient, GithubClientError, GithubRepoPermsProvider, MemoryGroupsCache,
    OrgDetails, Repository, Team,
};
use sg_service_worker::jobs::perms_syncer::AuthzProvider;

#[derive(Default)]
struct MockClient {
    calls: Mutex<usize>,
    scopes: Vec<String>,
    error: Option<String>,
}

#[async_trait]
impl GithubClient for MockClient {
    async fn list_repository_collaborators(
        &self,
        _owner: &str,
        _repo: &str,
        _page: i32,
        _affiliation: Option<&str>,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        Ok((Vec::new(), false))
    }

    async fn list_repository_teams(
        &self,
        _owner: &str,
        _repo: &str,
        _page: i32,
    ) -> Result<(Vec<Team>, bool), GithubClientError> {
        Ok((Vec::new(), false))
    }

    async fn list_organization_members(
        &self,
        _owner: &str,
        _page: i32,
        _admins_only: bool,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        Ok((Vec::new(), false))
    }

    async fn list_team_members(
        &self,
        _owner: &str,
        _team: &str,
        _page: i32,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        Ok((Vec::new(), false))
    }

    async fn get_organization(&self, _login: &str) -> Result<OrgDetails, GithubClientError> {
        Err(GithubClientError::new("unused"))
    }

    async fn get_repository(
        &self,
        _owner: &str,
        _name: &str,
    ) -> Result<Repository, GithubClientError> {
        Err(GithubClientError::new("unused"))
    }

    async fn get_authenticated_oauth_scopes(&self) -> Result<Vec<String>, GithubClientError> {
        *self.calls.lock().expect("lock") += 1;
        if let Some(error) = &self.error {
            return Err(GithubClientError::new(error.clone()));
        }
        Ok(self.scopes.clone())
    }
}

struct Scenario {
    name: &'static str,
    groups_cache: bool,
    scopes: &'static [&'static str],
    error: Option<&'static str>,
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_github_validate_matches_go_harness() {
    let scenarios = vec![
        Scenario {
            name: "cache-disabled",
            groups_cache: false,
            scopes: &[],
            error: None,
        },
        Scenario {
            name: "cache-enabled-error",
            groups_cache: true,
            scopes: &[],
            error: Some("scopes error"),
        },
        Scenario {
            name: "missing-org-scope",
            groups_cache: true,
            scopes: &[],
            error: None,
        },
        Scenario {
            name: "irrelevant-scopes",
            groups_cache: true,
            scopes: &["repo", "user"],
            error: None,
        },
        Scenario {
            name: "empty-scope-element",
            groups_cache: true,
            scopes: &[""],
            error: None,
        },
        Scenario {
            name: "read-org",
            groups_cache: true,
            scopes: &["read:org"],
            error: None,
        },
        Scenario {
            name: "write-org",
            groups_cache: true,
            scopes: &["write:org"],
            error: None,
        },
        Scenario {
            name: "admin-org",
            groups_cache: true,
            scopes: &["admin:org"],
            error: None,
        },
        Scenario {
            name: "other-plus-read-org",
            groups_cache: true,
            scopes: &["repo", "read:org"],
            error: None,
        },
        Scenario {
            name: "comma-no-space",
            groups_cache: true,
            scopes: &["read:org,user"],
            error: None,
        },
    ];

    for scenario in scenarios {
        let mock = std::sync::Arc::new(MockClient {
            calls: Mutex::new(0),
            scopes: scenario
                .scopes
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            error: scenario.error.map(str::to_owned),
        });
        let cache = if scenario.groups_cache {
            Some(std::sync::Arc::new(MemoryGroupsCache::new())
                as std::sync::Arc<
                    dyn sg_service_worker::jobs::authz_github::GroupsCache,
                >)
        } else {
            None
        };
        let provider = GithubRepoPermsProvider::new(
            "extsvc:github:1",
            "https://github.com/",
            "github",
            "github.com",
            mock.clone(),
            cache,
            false,
        );

        let error_text = match provider.validate_connection().await {
            Ok(()) => "none".to_owned(),
            Err(error) => error,
        };
        let calls = *mock.calls.lock().expect("lock");
        println!(
            "gvc scenario={} cache={} calls={calls} err={error_text}",
            scenario.name, scenario.groups_cache
        );
    }

    // `FetchAccount` always returns nil for GitHub.
    let provider = GithubRepoPermsProvider::new(
        "extsvc:github:1",
        "https://github.com/",
        "github",
        "github.com",
        std::sync::Arc::new(MockClient::default()),
        None,
        false,
    );
    let user = UserRecord {
        id: 1,
        username: "alice".to_owned(),
        created_at: time::OffsetDateTime::now_utc(),
    };
    let (account_text, error_text) = match provider.fetch_account(&user).await {
        Ok(Some(_)) => ("<set>", "none".to_owned()),
        Ok(None) => ("none", "none".to_owned()),
        Err(error) => ("none", error),
    };
    println!("gvc scenario=fetch-account account={account_text} err={error_text}");
}
