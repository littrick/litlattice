#![forbid(unsafe_code)]

//! `internal/repos/azuredevops.go`: the repo-updater Azure DevOps source.
//!
//! Enumerates the configured `projects` then `orgs`, one
//! `ListRepositoriesByProjectOrOrg` call each, applies the name/pattern
//! exclusions and maps every repository to the `types.Repo` shape used by the
//! syncer. Renames surface as a changed `Name` and removals as a repository
//! the source no longer yields; the syncer owns both effects.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::auth::BasicAuth;
use crate::azuredevops::{
    AzureDevOpsAuth, Client, ExcludedAzureDevOpsServerRepo, Repository, TYPE_AZUREDEVOPS,
};
use crate::exclude::{ExcludeRule, Excluder};
use crate::github::normalize_base_url;
use crate::{ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `schema.AzureDevOpsConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AzureDevOpsConnection {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub token: String,
    #[serde(rename = "gitURLType", default)]
    pub git_url_type: String,
    #[serde(default)]
    pub projects: Vec<String>,
    #[serde(default)]
    pub orgs: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<ExcludedAzureDevOpsServerRepo>,
}

/// `repos.AzureDevOpsSource`.
pub struct AzureDevOpsSource {
    service: SourceService,
    client: Client,
    service_id: String,
    config: AzureDevOpsConnection,
    excluder: Excluder,
}

impl AzureDevOpsSource {
    /// `repos.NewAzureDevOpsSource` without the HTTP client factory (the
    /// transport is injected as [`HttpDoer`]).
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: AzureDevOpsConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        // `NewAzureDevOpsSource` computes `serviceID` with
        // `extsvc.NormalizeBaseURL(cli.GetURL())`, which mutates the client's
        // URL in place (notably adding a trailing slash that `makeRepo` then
        // concatenates). Normalize before constructing the client.
        let base_url = crate::azuredevops::parse_urlx(&config.url)?.normalized();
        let auth = AzureDevOpsAuth::Basic(BasicAuth {
            username: config.username.clone(),
            password: config.token.clone(),
        });
        let client = Client::new(base_url, auth, doer);
        let service_id = normalize_base_url(client.base_url()).to_string();

        let rules: Vec<ExcludeRule> = config
            .exclude
            .iter()
            .map(|rule| ExcludeRule {
                name: Some(rule.name.clone()),
                pattern: Some(rule.pattern.clone()),
            })
            .collect();
        let excluder = Excluder::new(&rules)?;

        Ok(Self {
            service,
            client,
            service_id,
            config,
            excluder,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn excluder(&self) -> &Excluder {
        &self.excluder
    }

    /// `AzureDevOpsSource.makeRepo`.
    pub fn make_repo(&self, repo: &Repository) -> Result<Repo, String> {
        let urn = self.service.urn();
        let org = repo.get_organization()?;
        let full_url = crate::azuredevops::parse_urlx(&format!(
            "{}{}/{}/{}",
            self.client.base_url_string(),
            org,
            repo.project.name,
            repo.name
        ))?;
        // `path.Join(fullURL.Host, fullURL.Path)`: `url.URL.Host` keeps an
        // explicit port, unlike `Url::host_str`.
        let host = match full_url.url.port() {
            Some(port) => format!(
                "{}:{}",
                full_url.url.host_str().unwrap_or_default(),
                port
            ),
            None => full_url.url.host_str().unwrap_or_default().to_owned(),
        };
        let name = crate::gitlab::go_path_join(&host, full_url.url.path());

        let clone_url = if self.config.git_url_type == "ssh" {
            repo.ssh_url.clone()
        } else {
            repo.remote_url.clone()
        };

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn,
                clone_url,
            },
        );

        Ok(Repo {
            name,
            uri: crate::gitlab::go_path_join(&host, full_url.url.path()),
            external_repo: ExternalRepoSpec {
                id: repo.id.clone(),
                service_type: TYPE_AZUREDEVOPS.to_owned(),
                service_id: self.service_id.clone(),
            },
            sources,
            metadata: Some(
                serde_json::to_value(repo).expect("Azure DevOps repository metadata serialises"),
            ),
            fork: repo.is_fork,
            private: repo.project.visibility == "private",
            ..Repo::default()
        })
    }

    /// `processReposFromProjectOrOrg`.
    async fn process_repos_from_project_or_org(
        &self,
        name: &str,
        results: &mut Vec<SourceResult>,
    ) {
        let repos = match self
            .client
            .list_repositories_by_project_or_org(name)
            .await
        {
            Ok(repos) => repos,
            Err(err) => {
                results.push(SourceResult::error(err.to_string()));
                return;
            }
        };

        for repo in repos {
            let org = match repo.get_organization() {
                Ok(org) => org,
                Err(err) => {
                    results.push(SourceResult::error(err));
                    continue;
                }
            };
            let full_name = format!("{}/{}/{}", org, repo.project.name, repo.name);
            if self.excluder.should_exclude(&full_name) {
                continue;
            }
            match self.make_repo(&repo) {
                Ok(repo) => results.push(SourceResult::repo(repo)),
                Err(err) => {
                    results.push(SourceResult::error(err));
                    return;
                }
            }
        }
    }
}

#[async_trait]
impl Source for AzureDevOpsSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut results = Vec::new();
        for project in &self.config.projects {
            self.process_repos_from_project_or_org(project, &mut results)
                .await;
        }
        for org in &self.config.orgs {
            self.process_repos_from_project_or_org(org, &mut results)
                .await;
        }
        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        if self.client.is_azure_devops_services() {
            self.client
                .get_authorized_profile()
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        } else {
            // Azure DevOps Server connection checks are unsupported; the
            // baseline logs a warning and reports success.
            Ok(())
        }
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeDoer {
        responses: Mutex<Vec<crate::HttpResponse>>,
        requests: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl HttpDoer for FakeDoer {
        async fn do_request(
            &self,
            request: crate::HttpRequest,
        ) -> Result<crate::HttpResponse, String> {
            self.requests.lock().unwrap().push(request.url.clone());
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(crate::HttpResponse::default());
            }
            Ok(responses.remove(0))
        }
    }

    fn service(config: &str) -> SourceService {
        SourceService {
            id: 1,
            kind: crate::KIND_AZUREDEVOPS.to_owned(),
            display_name: "azuredevops".to_owned(),
            config: config.to_owned(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    fn repo_json() -> Vec<u8> {
        br#"{"value":[{"id":"repo-1","name":"myrepo","remoteURL":"https://dev.azure.com/myorg/myproj/_git/myrepo","url":"https://dev.azure.com/myorg/_apis/git/repositories/repo-1","sshUrl":"git@ssh.dev.azure.com:v3/myorg/myproj/myrepo","webUrl":"https://dev.azure.com/myorg/myproj/_git/myrepo","isDisabled":false,"isFork":true,"project":{"id":"p1","name":"myproj","state":"wellFormed","revision":3,"visibility":"private","url":"https://dev.azure.com/myorg/_apis/projects/p1"}}]}"#.to_vec()
    }

    #[tokio::test]
    async fn lists_projects_and_builds_the_expected_repo() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![crate::HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: repo_json(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let source = AzureDevOpsSource::new(
            service(r#"{"url":"https://dev.azure.com","username":"u","token":"t","projects":["myorg/myproj"],"gitURLType":"http"}"#),
            doer.clone(),
        )
        .unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        let repo = results[0].repo.clone().expect("repo");
        // `NewAzureDevOpsSource` normalizes the client URL with
        // `extsvc.NormalizeBaseURL` before `makeRepo` concatenates it, so a
        // base without a trailing slash still yields a clean name.
        assert_eq!(repo.name, "dev.azure.com/myorg/myproj/myrepo");
        assert_eq!(repo.uri, "dev.azure.com/myorg/myproj/myrepo");
        assert_eq!(repo.external_repo.id, "repo-1");
        assert_eq!(repo.external_repo.service_type, "azuredevops");
        assert_eq!(repo.external_repo.service_id, "https://dev.azure.com/");
        assert!(repo.fork);
        assert!(repo.private);
        assert_eq!(
            repo.sources[&"extsvc:azuredevops:1".to_owned()].clone_url,
            "https://dev.azure.com/myorg/myproj/_git/myrepo"
        );
        assert_eq!(
            doer.requests.lock().unwrap()[0],
            "https://dev.azure.com/myorg/myproj/_apis/git/repositories?api-version=7.0"
        );
    }

    #[tokio::test]
    async fn exclusions_are_applied_to_the_full_name() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![crate::HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: repo_json(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let source = AzureDevOpsSource::new(
            service(r#"{"url":"https://dev.azure.com","username":"u","token":"t","orgs":["myorg"],"exclude":[{"name":"myorg/myproj/myrepo"}]}"#),
            doer,
        )
        .unwrap();
        assert!(source.list_repos().await.is_empty());
    }

    #[tokio::test]
    async fn self_hosted_connection_check_makes_no_request() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        });
        let source = AzureDevOpsSource::new(
            service(r#"{"url":"https://ado.example.com","username":"u","token":"t"}"#),
            doer.clone(),
        )
        .unwrap();
        assert!(source.check_connection().await.is_ok());
        assert!(doer.requests.lock().unwrap().is_empty());
    }
}
