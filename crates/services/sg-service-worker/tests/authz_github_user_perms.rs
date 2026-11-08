#![forbid(unsafe_code)]

//! M9.8bb differential harness (Rust side): GitHub user-centric
//! `FetchUserPerms`.
//!
//! Mirrors `internal/authz/providers/github/authz_user_perms_harness_test.go`;
//! the two harnesses hardcode the same scenarios and are compared line for
//! line by `test_ws/p9-go/run_authz_github_user_perms_diff.sh`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::user_external_accounts::ExternalAccount;
use sg_encryption::Key;
use sg_service_worker::jobs::authz_github::{
    CachedGroup, GithubClientError, GithubUserClient, GroupsCache, MemoryGroupsCache,
    Org as GithubOrg, OrgDetailsAndMembership, Repository, Team as GithubTeam,
};
use sg_service_worker::jobs::authz_github_user::{
    github_oauth_providers, GithubOAuthProvider, GithubUserClientFactory, GithubUserPerms,
    GithubUserPermsProvider, OAuthTokenStore,
};
use sg_service_worker::jobs::perms_syncer::{
    ExternalUserPermissions, FetchPermsOptions, UserPermsError,
};

#[derive(Default, Clone)]
struct Page<T> {
    items: Vec<T>,
    has_next: bool,
    error: Option<GithubClientError>,
}

#[derive(Default)]
struct MockUserClient {
    calls: Mutex<Vec<String>>,
    affiliated: BTreeMap<i32, Page<Repository>>,
    org_details:
        BTreeMap<i32, Page<sg_service_worker::jobs::authz_github::OrgDetailsAndMembership>>,
    orgs: BTreeMap<i32, Page<GithubOrg>>,
    teams: BTreeMap<i32, Page<GithubTeam>>,
    org_repos: BTreeMap<String, BTreeMap<i32, Page<Repository>>>,
    team_repos: BTreeMap<String, BTreeMap<i32, Page<Repository>>>,
}

impl MockUserClient {
    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn page<T>(
        pages: Option<&BTreeMap<i32, Page<T>>>,
        page: i32,
    ) -> Result<(Vec<T>, bool), GithubClientError>
    where
        T: Clone,
    {
        let Some(page) = pages.and_then(|pages| pages.get(&page)) else {
            return Ok((Vec::new(), false));
        };
        if let Some(err) = &page.error {
            return Err(err.clone());
        }
        Ok((page.items.clone(), page.has_next))
    }

    fn base() -> Self {
        let mut mock = Self::default();
        mock.affiliated.insert(
            1,
            Page {
                items: vec![repo("R_a"), repo("R_b")],
                has_next: false,
                error: None,
            },
        );
        mock.org_details.insert(
            1,
            Page {
                items: vec![org_details("acme", "read")],
                has_next: false,
                error: None,
            },
        );
        mock.orgs.insert(
            1,
            Page {
                items: vec![GithubOrg {
                    login: "acme".to_owned(),
                }],
                has_next: false,
                error: None,
            },
        );
        mock.teams.insert(
            1,
            Page {
                items: vec![],
                has_next: false,
                error: None,
            },
        );
        mock.org_repos.insert(
            "acme".to_owned(),
            BTreeMap::from([(
                1,
                Page {
                    items: vec![repo("R_o1"), repo("R_o2")],
                    has_next: false,
                    error: None,
                },
            )]),
        );
        mock
    }
}

fn repo(id: &str) -> Repository {
    Repository {
        id: id.to_owned(),
        visibility: "private".to_owned(),
    }
}

fn org_details(login: &str, permission: &str) -> OrgDetailsAndMembership {
    OrgDetailsAndMembership {
        details: Some(sg_service_worker::jobs::authz_github::OrgDetails {
            login: login.to_owned(),
            default_repository_permission: permission.to_owned(),
        }),
        membership: None,
    }
}

fn api_error(url: &str, code: u16, message: &str) -> GithubClientError {
    GithubClientError {
        message: format!("request to {url} returned status {code}: {message}"),
        not_found: code == 404,
        status: code,
        api_message: message.to_owned(),
        documentation_url: String::new(),
    }
}

const USER_REPOS_URL: &str = "https://api.github.com/user/repos";
const ORG_REPOS_URL: &str = "https://api.github.com/orgs/acme/repos";
const ORG_DETAILS_URL: &str = "https://api.github.com/orgs/acme";

fn join_names(values: &[String]) -> String {
    values.join(",")
}

fn join_values(values: &Option<Vec<String>>) -> String {
    match values {
        None => "<nil>".to_owned(),
        Some(values) if values.is_empty() => "<empty>".to_owned(),
        Some(values) => values.join(","),
    }
}

fn sort_repo_ids(perms: &Option<ExternalUserPermissions>) -> String {
    match perms {
        None => "<empty>".to_owned(),
        Some(perms) if perms.exacts.is_empty() => "<empty>".to_owned(),
        Some(perms) => {
            let mut ids = perms.exacts.clone();
            ids.sort();
            ids.join(",")
        }
    }
}

#[async_trait]
impl GithubUserClient for MockUserClient {
    async fn list_affiliated_repositories(
        &self,
        page: i32,
        affiliations: &[&str],
    ) -> Result<(Vec<Repository>, bool), GithubClientError> {
        self.record(format!(
            "list-affiliated-repositories page={page} affiliations={}",
            join_names(
                &affiliations
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
            )
        ));
        Self::page(Some(&self.affiliated), page)
    }

    async fn get_authenticated_user_orgs_details_and_membership(
        &self,
        page: i32,
    ) -> Result<(Vec<OrgDetailsAndMembership>, bool), GithubClientError> {
        self.record(format!(
            "get-authenticated-user-orgs-details-and-membership page={page}"
        ));
        Self::page(Some(&self.org_details), page)
    }

    async fn get_authenticated_user_orgs(
        &self,
        page: i32,
    ) -> Result<(Vec<GithubOrg>, bool), GithubClientError> {
        self.record(format!("get-authenticated-user-orgs page={page}"));
        Self::page(Some(&self.orgs), page)
    }

    async fn get_authenticated_user_teams(
        &self,
        page: i32,
    ) -> Result<(Vec<GithubTeam>, bool), GithubClientError> {
        self.record(format!("get-authenticated-user-teams page={page}"));
        Self::page(Some(&self.teams), page)
    }

    async fn list_org_repositories(
        &self,
        org: &str,
        page: i32,
        repo_type: &str,
    ) -> Result<(Vec<Repository>, bool), GithubClientError> {
        self.record(format!(
            "list-org-repositories org={org} page={page} type={repo_type}"
        ));
        Self::page(self.org_repos.get(org), page)
    }

    async fn list_team_repositories(
        &self,
        org: &str,
        team: &str,
        page: i32,
    ) -> Result<(Vec<Repository>, bool), GithubClientError> {
        self.record(format!(
            "list-team-repositories org={org} team={team} page={page}"
        ));
        Self::page(self.team_repos.get(&format!("{org}/{team}")), page)
    }
}

struct MockFactory {
    client: Arc<MockUserClient>,
}

impl GithubUserClientFactory for MockFactory {
    fn with_oauth_token(
        &self,
        _token: Arc<sg_service_worker::jobs::authz_github_user::OAuthBearerToken>,
    ) -> Arc<dyn GithubUserClient> {
        self.client.clone()
    }
}

/// The refresh path is compared by `authz_github_user_refresh.rs`; these
/// scenarios never refresh (no expiry, no 401), so the store must stay unused.
struct UnusedStore;

#[async_trait]
impl OAuthTokenStore for UnusedStore {
    async fn get(&self, _id: i32) -> Result<Option<ExternalAccount>, String> {
        panic!("token store must not be used by the user perms harness")
    }

    async fn update(&self, _account: &ExternalAccount, _auth_data: &str) -> Result<(), String> {
        panic!("token store must not be used by the user perms harness")
    }
}

fn harness_account(
    id: i32,
    service_type: &str,
    service_id: &str,
    auth_data: Option<&str>,
    account_data: Option<&str>,
) -> ExternalAccount {
    ExternalAccount {
        id,
        user_id: 1,
        service_type: service_type.to_owned(),
        service_id: service_id.to_owned(),
        client_id: "client".to_owned(),
        account_id: "42".to_owned(),
        auth_data: auth_data.map(str::to_owned),
        account_data: account_data.map(str::to_owned),
        encryption_key_id: String::new(),
        expired: false,
    }
}

type Configure = Box<dyn Fn(&mut MockUserClient)>;

struct Scenario {
    name: &'static str,
    cache: bool,
    sync_internal: bool,
    invalidate: bool,
    seed: Vec<CachedGroup>,
    oauth_urls: Vec<&'static str>,
    account: ExternalAccount,
    configure: Option<Configure>,
}

fn default_account() -> ExternalAccount {
    harness_account(
        1,
        "github",
        "https://github.com/",
        Some(r#"{"access_token":"TOKEN"}"#),
        None,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_github_user_perms_matches_go_harness() {
    let scenarios = vec![
        Scenario {
            name: "nocache",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([
                    (
                        1,
                        Page {
                            items: vec![repo("R_a"), repo("R_b")],
                            has_next: true,
                            error: None,
                        },
                    ),
                    (
                        2,
                        Page {
                            items: vec![repo("R_c")],
                            has_next: false,
                            error: None,
                        },
                    ),
                ]);
            })),
        },
        Scenario {
            name: "nocache-internal",
            cache: false,
            sync_internal: true,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![repo("R_a")],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.orgs = BTreeMap::from([
                    (
                        1,
                        Page {
                            items: vec![
                                GithubOrg {
                                    login: "acme".to_owned(),
                                },
                                GithubOrg {
                                    login: "beta".to_owned(),
                                },
                            ],
                            has_next: true,
                            error: None,
                        },
                    ),
                    (
                        2,
                        Page {
                            items: vec![GithubOrg {
                                login: "gamma".to_owned(),
                            }],
                            has_next: false,
                            error: None,
                        },
                    ),
                ]);
                mock.org_repos = BTreeMap::from([
                    (
                        "acme".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: vec![repo("R_i1")],
                                has_next: false,
                                error: None,
                            },
                        )]),
                    ),
                    (
                        "beta".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: vec![repo("R_i2")],
                                has_next: false,
                                error: None,
                            },
                        )]),
                    ),
                    (
                        "gamma".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: vec![repo("R_i3")],
                                has_next: false,
                                error: None,
                            },
                        )]),
                    ),
                ]);
            })),
        },
        Scenario {
            name: "affil-error",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([
                    (
                        1,
                        Page {
                            items: vec![repo("R_a")],
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
            name: "unauthorized",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![],
                        has_next: false,
                        error: Some(api_error(USER_REPOS_URL, 401, "Bad credentials")),
                    },
                )]);
            })),
        },
        Scenario {
            name: "suspended",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![],
                        has_next: false,
                        error: Some(api_error(USER_REPOS_URL, 403, "account was suspended")),
                    },
                )]);
            })),
        },
        Scenario {
            name: "ratelimit",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![],
                        has_next: false,
                        error: Some(api_error(
                            USER_REPOS_URL,
                            403,
                            "API rate limit exceeded for user ID 1.",
                        )),
                    },
                )]);
            })),
        },
        Scenario {
            name: "internal-org-error",
            cache: false,
            sync_internal: true,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![repo("R_a")],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.orgs = BTreeMap::from([
                    (
                        1,
                        Page {
                            items: vec![GithubOrg {
                                login: "acme".to_owned(),
                            }],
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
            name: "internal-repo-error",
            cache: false,
            sync_internal: true,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.affiliated = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![repo("R_a")],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.orgs = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![
                            GithubOrg {
                                login: "acme".to_owned(),
                            },
                            GithubOrg {
                                login: "beta".to_owned(),
                            },
                        ],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.org_repos = BTreeMap::from([
                    (
                        "acme".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: vec![repo("R_i1")],
                                has_next: false,
                                error: None,
                            },
                        )]),
                    ),
                    (
                        "beta".to_owned(),
                        BTreeMap::from([(
                            1,
                            Page {
                                items: vec![],
                                has_next: false,
                                error: Some(GithubClientError::new("boom")),
                            },
                        )]),
                    ),
                ]);
            })),
        },
        Scenario {
            name: "cache-org",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: None,
        },
        Scenario {
            name: "cache-team",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_details = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.teams = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![
                            GithubTeam {
                                slug: "t1".to_owned(),
                                repos_count: 2,
                                organization: Some(GithubOrg {
                                    login: "acme".to_owned(),
                                }),
                            },
                            GithubTeam {
                                slug: "t2".to_owned(),
                                repos_count: 0,
                                organization: Some(GithubOrg {
                                    login: "acme".to_owned(),
                                }),
                            },
                            GithubTeam {
                                slug: "t3".to_owned(),
                                repos_count: 1,
                                organization: None,
                            },
                        ],
                        has_next: false,
                        error: None,
                    },
                )]);
                mock.team_repos = BTreeMap::from([(
                    "acme/t1".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![repo("R_t1")],
                            has_next: false,
                            error: None,
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "cache-seeded",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![CachedGroup {
                org: "acme".to_owned(),
                team: String::new(),
                repositories: Some(vec!["R_seed".to_owned()]),
                users: None,
            }],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: None,
        },
        Scenario {
            name: "cache-seeded-empty",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![CachedGroup {
                org: "acme".to_owned(),
                team: String::new(),
                repositories: Some(vec![]),
                users: None,
            }],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: None,
        },
        Scenario {
            name: "cache-partial-users",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![CachedGroup {
                org: "acme".to_owned(),
                team: String::new(),
                repositories: None,
                users: Some(vec!["other".to_owned()]),
            }],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_repos = BTreeMap::from([(
                    "acme".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![repo("R_o1")],
                            has_next: false,
                            error: None,
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "cache-403",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_repos = BTreeMap::from([(
                    "acme".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![],
                            has_next: false,
                            error: Some(api_error(ORG_REPOS_URL, 403, "Forbidden")),
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "cache-404",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_repos = BTreeMap::from([(
                    "acme".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![],
                            has_next: false,
                            error: Some(api_error(ORG_REPOS_URL, 404, "Not Found")),
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "cache-error",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_repos = BTreeMap::from([(
                    "acme".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![],
                            has_next: false,
                            error: Some(api_error(ORG_REPOS_URL, 500, "boom")),
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "cache-groups-error",
            cache: true,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_details = BTreeMap::from([(
                    1,
                    Page {
                        items: vec![],
                        has_next: false,
                        error: Some(api_error(ORG_DETAILS_URL, 500, "boom")),
                    },
                )]);
            })),
        },
        Scenario {
            name: "invalidate",
            cache: true,
            sync_internal: false,
            invalidate: true,
            seed: vec![CachedGroup {
                org: "acme".to_owned(),
                team: String::new(),
                repositories: Some(vec!["R_old".to_owned()]),
                users: Some(vec!["42".to_owned()]),
            }],
            oauth_urls: vec!["https://github.com"],
            account: default_account(),
            configure: Some(Box::new(|mock| {
                mock.org_repos = BTreeMap::from([(
                    "acme".to_owned(),
                    BTreeMap::from([(
                        1,
                        Page {
                            items: vec![repo("R_o1")],
                            has_next: false,
                            error: None,
                        },
                    )]),
                )]);
            })),
        },
        Scenario {
            name: "wrong-service",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: harness_account(
                1,
                "gitlab",
                "https://gitlab.com/",
                Some(r#"{"access_token":"TOKEN"}"#),
                None,
            ),
            configure: None,
        },
        Scenario {
            name: "no-token",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: harness_account(1, "github", "https://github.com/", None, None),
            configure: None,
        },
        Scenario {
            name: "no-oauth",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec![],
            account: default_account(),
            configure: None,
        },
        Scenario {
            name: "oauth-trailing-slash",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com/"],
            account: default_account(),
            configure: None,
        },
        Scenario {
            name: "oauth-empty-url",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec![""],
            account: default_account(),
            configure: None,
        },
        Scenario {
            name: "account-data",
            cache: false,
            sync_internal: false,
            invalidate: false,
            seed: vec![],
            oauth_urls: vec!["https://github.com"],
            account: harness_account(
                1,
                "github",
                "https://github.com/",
                Some(r#"{"access_token":"TOKEN"}"#),
                Some(r#"{"login":"u"}"#),
            ),
            configure: None,
        },
    ];

    for scenario in scenarios {
        let mut mock = MockUserClient::base();
        if let Some(configure) = &scenario.configure {
            configure(&mut mock);
        }
        let mock = Arc::new(mock);
        let factory = Arc::new(MockFactory {
            client: mock.clone(),
        });

        let cache_impl = Arc::new(MemoryGroupsCache::new());
        for group in &scenario.seed {
            cache_impl.seed(group);
        }
        let cache: Option<Arc<dyn GroupsCache>> = if scenario.cache {
            Some(cache_impl.clone())
        } else {
            None
        };

        // Mirror the Go harness's `conf.Mock`: a site configuration carrying
        // the same `auth.providers` entries (one per URL).
        let auth_providers: Vec<serde_json::Value> = scenario
            .oauth_urls
            .iter()
            .map(|url| {
                serde_json::json!({
                    "type": "github",
                    "url": url,
                    "clientID": "client",
                    "clientSecret": "secret",
                })
            })
            .collect();
        let site = SiteConfiguration::parse(
            &serde_json::json!({"auth.providers": auth_providers}).to_string(),
        )
        .expect("site config");
        let oauth: Vec<GithubOAuthProvider> = github_oauth_providers(&site);

        let provider = GithubUserPerms::new(
            "https://github.com/",
            "github",
            factory,
            cache,
            scenario.sync_internal,
            oauth,
            Arc::new(UnusedStore),
            None as Option<Arc<dyn Key>>,
            reqwest::Client::new(),
        );
        let opts = FetchPermsOptions {
            invalidate_caches: scenario.invalidate,
        };
        let name = scenario.name;

        let (status, result, error_text) =
            match provider.fetch_user_perms(&scenario.account, &opts).await {
                Ok(perms) => ("ok", sort_repo_ids(&perms), "none".to_owned()),
                Err(UserPermsError::Failed { message, partial }) => {
                    ("failed", sort_repo_ids(&partial), message)
                }
                Err(UserPermsError::Unauthorized { message }) => {
                    ("unauthorized", "<ignored>".to_owned(), message)
                }
                Err(UserPermsError::Forbidden { message }) => {
                    ("forbidden", "<ignored>".to_owned(), message)
                }
                Err(UserPermsError::AccountSuspended { message }) => {
                    ("account_suspended", "<ignored>".to_owned(), message)
                }
                Err(UserPermsError::Temporary { message }) => {
                    ("temporary", "<ignored>".to_owned(), message)
                }
                Err(UserPermsError::Unimplemented { message }) => {
                    ("unimplemented", "<ignored>".to_owned(), message)
                }
            };
        println!("gup scenario={name} status={status} result={result} err={error_text}");

        for call in mock.calls.lock().unwrap().iter() {
            println!("gup scenario={name} call={call}");
        }
        if scenario.cache {
            for group in cache_impl.groups() {
                println!(
                    "gup scenario={name} cache={} repos={} users={}",
                    group.key(),
                    join_values(&group.repositories),
                    join_values(&group.users)
                );
            }
        } else {
            println!("gup scenario={name} cache=<none>");
        }
    }
}
