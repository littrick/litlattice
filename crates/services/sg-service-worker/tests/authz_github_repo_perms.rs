#![forbid(unsafe_code)]

//! M9.8aw differential harness (Rust side): GitHub repository-centric
//! `FetchRepoPerms`.
//!
//! Mirrors `internal/authz/providers/github/authz_repo_perms_harness_test.go`.
//! The two harnesses hardcode the same scenarios; outputs are compared line
//! for line by `test_ws/p9-go/run_authz_github_repo_perms_diff.sh`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_service_worker::jobs::authz_github::{
    CachedGroup, Collaborator, GithubClient, GithubClientError, GithubRepoPermsProvider,
    GroupsCache, MemoryGroupsCache, OrgDetails, Repository, Team,
};
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, FetchError, FetchPermsOptions, FetchRepo,
};

#[derive(Default)]
struct Page<T> {
    items: Vec<T>,
    has_next: bool,
    error: Option<GithubClientError>,
}

#[derive(Default)]
struct MockClient {
    calls: Mutex<Vec<String>>,
    collaborators: BTreeMap<i32, Page<Collaborator>>,
    repo_teams: BTreeMap<i32, Page<Team>>,
    org_members: BTreeMap<i32, Page<Collaborator>>,
    team_members: BTreeMap<String, BTreeMap<i32, Page<Collaborator>>>,
    org: Option<OrgDetails>,
    org_not_found: bool,
    org_error: Option<GithubClientError>,
    repo: Option<Repository>,
    repo_error: Option<GithubClientError>,
}

impl MockClient {
    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn base() -> Self {
        let mut mock = Self::default();
        mock.collaborators.insert(
            1,
            Page {
                items: collabs(&[11, 12]),
                has_next: false,
                error: None,
            },
        );
        mock.org_members.insert(
            1,
            Page {
                items: collabs(&[21, 22]),
                has_next: false,
                error: None,
            },
        );
        mock.org = Some(OrgDetails {
            login: "acme".to_owned(),
            default_repository_permission: "read".to_owned(),
        });
        mock.repo = Some(Repository {
            id: "R_repo".to_owned(),
            visibility: "private".to_owned(),
        });
        mock
    }
}

fn collabs(ids: &[i64]) -> Vec<Collaborator> {
    ids.iter()
        .map(|id| Collaborator { database_id: *id })
        .collect()
}

#[async_trait]
impl GithubClient for MockClient {
    async fn list_repository_collaborators(
        &self,
        owner: &str,
        repo: &str,
        page: i32,
        affiliation: Option<&str>,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        self.record(format!(
            "list-repository-collaborators owner={owner} repo={repo} page={page} affiliation={}",
            affiliation.unwrap_or("")
        ));
        let Some(page) = self.collaborators.get(&page) else {
            return Ok((Vec::new(), false));
        };
        if let Some(err) = &page.error {
            return Err(err.clone());
        }
        Ok((page.items.clone(), page.has_next))
    }

    async fn list_repository_teams(
        &self,
        owner: &str,
        repo: &str,
        page: i32,
    ) -> Result<(Vec<Team>, bool), GithubClientError> {
        self.record(format!(
            "list-repository-teams owner={owner} repo={repo} page={page}"
        ));
        let Some(page) = self.repo_teams.get(&page) else {
            return Ok((Vec::new(), false));
        };
        if let Some(err) = &page.error {
            return Err(err.clone());
        }
        Ok((page.items.clone(), page.has_next))
    }

    async fn list_organization_members(
        &self,
        owner: &str,
        page: i32,
        admins_only: bool,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        self.record(format!(
            "list-organization-members owner={owner} page={page} admins_only={admins_only}"
        ));
        let Some(page) = self.org_members.get(&page) else {
            return Ok((Vec::new(), false));
        };
        if let Some(err) = &page.error {
            return Err(err.clone());
        }
        Ok((page.items.clone(), page.has_next))
    }

    async fn list_team_members(
        &self,
        owner: &str,
        team: &str,
        page: i32,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        self.record(format!(
            "list-team-members owner={owner} team={team} page={page}"
        ));
        let Some(pages) = self.team_members.get(team) else {
            return Ok((Vec::new(), false));
        };
        let Some(page) = pages.get(&page) else {
            return Ok((Vec::new(), false));
        };
        if let Some(err) = &page.error {
            return Err(err.clone());
        }
        Ok((page.items.clone(), page.has_next))
    }

    async fn get_organization(&self, login: &str) -> Result<OrgDetails, GithubClientError> {
        self.record(format!("get-organization login={login}"));
        if self.org_not_found {
            return Err(GithubClientError::not_found());
        }
        if let Some(err) = &self.org_error {
            return Err(err.clone());
        }
        self.org
            .clone()
            .ok_or_else(|| GithubClientError::new("no org"))
    }

    async fn get_repository(
        &self,
        owner: &str,
        name: &str,
    ) -> Result<Repository, GithubClientError> {
        self.record(format!("get-repository owner={owner} name={name}"));
        if let Some(err) = &self.repo_error {
            return Err(err.clone());
        }
        self.repo
            .clone()
            .ok_or_else(|| GithubClientError::new("no repository"))
    }
}

fn join_ids(ids: &[String]) -> String {
    if ids.is_empty() {
        "<empty>".to_owned()
    } else {
        ids.join(",")
    }
}

fn join_values(values: &Option<Vec<String>>) -> String {
    match values {
        None => "<nil>".to_owned(),
        Some(values) if values.is_empty() => "<empty>".to_owned(),
        Some(values) => values.join(","),
    }
}

fn default_repo() -> FetchRepo {
    FetchRepo {
        uri: "github.com/acme/repo".to_owned(),
        external_repo_id: "R_repo".to_owned(),
        service_type: "github".to_owned(),
        service_id: "https://github.com/".to_owned(),
    }
}

type Configure = Box<dyn Fn(&mut MockClient)>;

struct Scenario {
    name: &'static str,
    enable_internal: bool,
    cache: bool,
    invalidate: bool,
    seed: Vec<CachedGroup>,
    repo: FetchRepo,
    configure: Option<Configure>,
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_github_repo_perms_matches_go_harness() {
    let scenarios = vec![
        Scenario {
            name: "nocache",
            enable_internal: false,
            cache: false,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.collaborators = BTreeMap::from([
                    (
                        1,
                        Page {
                            items: collabs(&[11, 12]),
                            has_next: true,
                            error: None,
                        },
                    ),
                    (
                        2,
                        Page {
                            items: collabs(&[12, 13]),
                            has_next: false,
                            error: None,
                        },
                    ),
                ]);
            })),
        },
        Scenario {
            name: "cache-basic",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: None,
        },
        Scenario {
            name: "cache-dedup",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.collaborators = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[11, 11]),
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[21, 21]),
                        has_next: false,
                        error: None,
                    },
                )]);
            })),
        },
        Scenario {
            name: "cache-restricted",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                if let Some(org) = mock.org.as_mut() {
                    org.default_repository_permission = "none".to_owned();
                }
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[41]),
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.repo_teams = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![
                            Team {
                                slug: "t1".to_owned(),
                                repos_count: 1,
                                organization: None,
                            },
                            Team {
                                slug: "t2".to_owned(),
                                repos_count: 2,
                                organization: None,
                            },
                        ],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.team_members = BTreeMap::from([
                    (
                        "t1".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: collabs(&[31]),
                                has_next: false,
                                error: None,
                            },
                        )]),
                    ),
                    (
                        "t2".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: collabs(&[32, 33]),
                                has_next: false,
                                error: None,
                            },
                        )]),
                    ),
                ]);
            })),
        },
        Scenario {
            name: "internal-on",
            enable_internal: true,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                if let Some(org) = mock.org.as_mut() {
                    org.default_repository_permission = "none".to_owned();
                }
                if let Some(repo) = mock.repo.as_mut() {
                    repo.visibility = "internal".to_owned();
                }
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[51]),
                        has_next: false,
                        error: None,
                    },
                )]);
            })),
        },
        Scenario {
            name: "internal-private",
            enable_internal: true,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                if let Some(org) = mock.org.as_mut() {
                    org.default_repository_permission = "none".to_owned();
                }
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[52]),
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.repo_teams = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![Team {
                            slug: "t1".to_owned(),
                            repos_count: 1,
                            organization: None,
                        }],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.team_members = BTreeMap::from([(
                    "t1".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: collabs(&[53]),
                            has_next: false,
                            error: None,
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "invalidate",
            enable_internal: false,
            cache: true,
            invalidate: true,
            seed: vec![CachedGroup {
                org: "acme".to_owned(),
                team: String::new(),
                repositories: Some(vec!["R_old".to_owned()]),
                users: Some(vec!["61".to_owned()]),
            }],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[62]),
                        has_next: false,
                        error: None,
                    },
                )]);
            })),
        },
        Scenario {
            name: "partial",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![CachedGroup {
                org: "acme".to_owned(),
                team: String::new(),
                repositories: Some(vec!["R_old".to_owned()]),
                users: None,
            }],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[71]),
                        has_next: false,
                        error: None,
                    },
                )]);
            })),
        },
        Scenario {
            name: "no-org",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.org_not_found = true;
            })),
        },
        Scenario {
            name: "wrong-service",
            enable_internal: false,
            cache: false,
            invalidate: false,
            seed: vec![],
            repo: FetchRepo {
                uri: "gitlab.com/acme/repo".to_owned(),
                external_repo_id: "R_repo".to_owned(),
                service_type: "gitlab".to_owned(),
                service_id: "https://gitlab.com/".to_owned(),
            },
            configure: None,
        },
        Scenario {
            name: "invalid-uri",
            enable_internal: false,
            cache: false,
            invalidate: false,
            seed: vec![],
            repo: FetchRepo {
                uri: "github.com/acme".to_owned(),
                external_repo_id: "R_repo".to_owned(),
                service_type: "github".to_owned(),
                service_id: "https://github.com/".to_owned(),
            },
            configure: None,
        },
        Scenario {
            name: "collab-error",
            enable_internal: false,
            cache: false,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.collaborators = BTreeMap::from([
                    (
                        1,
                        Page {
                            items: collabs(&[11]),
                            has_next: true,
                            error: None,
                        },
                    ),
                    (
                        2,
                        Page {
                            items: vec![],
                            has_next: false,
                            error: Some(GithubClientError::new("boom")),
                        },
                    ),
                ]);
            })),
        },
        Scenario {
            name: "org-error",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.org_error = Some(GithubClientError::new("nope"));
            })),
        },
        Scenario {
            name: "team-member-error",
            enable_internal: false,
            cache: true,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                if let Some(org) = mock.org.as_mut() {
                    org.default_repository_permission = "none".to_owned();
                }
                mock.org_members = BTreeMap::from([(
                    1,
                    Page {
                        items: collabs(&[41]),
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.repo_teams = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![Team {
                            slug: "t1".to_owned(),
                            repos_count: 1,
                            organization: None,
                        }],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.team_members = BTreeMap::from([(
                    "t1".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![],
                            has_next: false,
                            error: Some(GithubClientError::new("kaput")),
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "collab-notfound",
            enable_internal: false,
            cache: false,
            invalidate: false,
            seed: vec![],
            repo: default_repo(),
            configure: Some(Box::new(|mock| {
                mock.collaborators = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![],
                        has_next: false,
                        error: Some(GithubClientError::not_found()),
                    },
                )]);
            })),
        },
    ];

    for scenario in scenarios {
        let mut mock = MockClient::base();
        if let Some(configure) = &scenario.configure {
            configure(&mut mock);
        }
        let mock = Arc::new(mock);

        let cache_impl = Arc::new(MemoryGroupsCache::new());
        for group in &scenario.seed {
            cache_impl.seed(group);
        }
        let cache: Option<Arc<dyn GroupsCache>> = if scenario.cache {
            Some(cache_impl.clone())
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
            scenario.enable_internal,
        );
        let opts = FetchPermsOptions {
            invalidate_caches: scenario.invalidate,
        };
        let name = scenario.name;

        match provider.fetch_repo_perms(&scenario.repo, &opts).await {
            Ok(ids) => println!("gh op={name} status=ok result={} err=none", join_ids(&ids)),
            Err(FetchError::NotFound) => {
                println!("gh op={name} status=notfound result=<empty> err=none")
            }
            Err(FetchError::Unimplemented) => {
                println!("gh op={name} status=failed result=<empty> err=unimplemented")
            }
            Err(FetchError::Failed {
                message,
                partial_account_ids,
            }) => println!(
                "gh op={name} status=failed result={} err={message}",
                join_ids(&partial_account_ids)
            ),
        }

        for call in mock.calls.lock().unwrap().iter() {
            println!("gh op={name} call={call}");
        }
        if scenario.cache {
            for group in cache_impl.groups() {
                println!(
                    "gh op={name} cache={} repos={} users={}",
                    group.key(),
                    join_values(&group.repositories),
                    join_values(&group.users)
                );
            }
        } else {
            println!("gh op={name} cache=<none>");
        }
    }
}
