#![forbid(unsafe_code)]

//! `internal/repos/gerrit.go`: the repo-updater Gerrit source.
//!
//! Fetches the instance's SSH endpoint once, pages through `CODE` projects,
//! sorts each page's project map by key (the baseline sorts because Gerrit
//! returns a map), applies the allow/deny project sets, and maps every project
//! to the `types.Repo` shape. The deny set wins over the allow set.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use url::Url;

use crate::gerrit::{
    AccountCredentials, Client, GerritConnection, ListProjectsArgs, Pagination, Project, TYPE_GERRIT,
};
use crate::github::normalize_base_url;
use crate::reposource::gerrit_repo_name;
use crate::{ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `repos.GerritSource`.
pub struct GerritSource {
    service: SourceService,
    client: Client,
    service_id: String,
    config: GerritConnection,
    per_page: i64,
    private: bool,
    allowed_projects: HashSet<String>,
    /// Takes precedence over `allowed_projects`.
    disallowed_projects: HashSet<String>,
    ssh: bool,
    repository_path_pattern: String,
}

impl GerritSource {
    /// `repos.NewGerritSource` without the HTTP client factory (the transport
    /// is injected as [`HttpDoer`]).
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: GerritConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        // `NewGerritSource` computes `serviceID` with
        // `extsvc.NormalizeBaseURL(cli.GetURL())`, which mutates the client's
        // URL in place. A configured path prefix such as `/gerrit` therefore
        // becomes `/gerrit/`, and later relative requests resolve under it.
        let base_url = normalize_base_url(&Url::parse(&config.url).map_err(|err| err.to_string())?);
        let client = Client::new(
            base_url,
            &AccountCredentials {
                username: config.username.clone(),
                password: config.password.clone(),
            },
            doer,
        );
        let service_id = normalize_base_url(client.url()).to_string();

        let allowed_projects = config.projects.iter().cloned().collect();
        let disallowed_projects = config
            .exclude
            .iter()
            .map(|project| project.name.clone())
            .collect();

        let private = config.authorization.is_some();
        let ssh = config.git_url_type == "ssh";
        let repository_path_pattern = config.repository_path_pattern.clone();

        Ok(Self {
            service,
            client,
            service_id,
            config,
            per_page: 100,
            private,
            allowed_projects,
            disallowed_projects,
            ssh,
            repository_path_pattern,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// `GerritSource.makeRepo`.
    pub fn make_repo(
        &self,
        project: &Project,
        instance_http_url: &Url,
        ssh_hostname: &str,
        ssh_port: i64,
    ) -> Repo {
        let mut url = instance_http_url.clone();
        let _ = url.set_username("");
        let _ = url.set_password(None);

        // Gerrit percent-encodes slashes in project IDs.
        let decoded_name = project.id.replace("%2F", "/");
        url.set_path(&crate::gitlab::go_path_join("a", &decoded_name));

        let mut project = project.clone();
        project.http_url_to_repo = url.to_string();
        project.ssh_url_to_repo = format!("ssh://{ssh_hostname}:{ssh_port}/{decoded_name}");

        let clone_url = if self.ssh {
            project.ssh_url_to_repo.clone()
        } else {
            project.http_url_to_repo.clone()
        };

        let host = host_with_port(&url);
        let urn = self.service.urn();

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn,
                clone_url,
            },
        );

        Repo {
            name: gerrit_repo_name(&self.repository_path_pattern, &host, &decoded_name),
            uri: gerrit_repo_name("", &host, &decoded_name),
            description: project.description.clone(),
            external_repo: ExternalRepoSpec {
                id: project.id.clone(),
                service_type: TYPE_GERRIT.to_owned(),
                service_id: self.service_id.clone(),
            },
            fork: !project.parent.is_empty(),
            sources,
            metadata: Some(
                serde_json::to_value(&project).expect("Gerrit project metadata serialises"),
            ),
            private: self.private,
            ..Repo::default()
        }
    }
}

/// `url.URL.Host` (hostname plus an explicit port).
fn host_with_port(url: &Url) -> String {
    match url.port() {
        Some(port) => format!("{}:{}", url.host_str().unwrap_or_default(), port),
        None => url.host_str().unwrap_or_default().to_owned(),
    }
}

#[async_trait]
impl Source for GerritSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut results = Vec::new();
        let mut cursor = Pagination {
            per_page: self.per_page,
            page: 1,
            skip: 0,
        };

        let (ssh_hostname, ssh_port) = match self.client.get_ssh_info().await {
            Ok(info) => info,
            Err(err) => {
                results.push(SourceResult::error(format!(
                    "failed to get ssh info for Gerrit instance: {err}"
                )));
                return results;
            }
        };

        loop {
            let (page, next_page) = match self
                .client
                .list_projects(&ListProjectsArgs {
                    cursor,
                    only_code_projects: true,
                })
                .await
            {
                Ok(page) => page,
                Err(err) => {
                    results.push(SourceResult::error(err.to_string()));
                    return results;
                }
            };

            // Gerrit responds with a map, so sort the keys for stable output.
            let mut keys: Vec<String> = page.keys().cloned().collect();
            keys.sort();

            for key in keys {
                if self.disallowed_projects.contains(&key) {
                    continue;
                }
                if !self.allowed_projects.is_empty() && !self.allowed_projects.contains(&key) {
                    continue;
                }
                let Some(project) = page.get(&key) else {
                    continue;
                };
                results.push(SourceResult::repo(self.make_repo(
                    project,
                    self.client.url(),
                    &ssh_hostname,
                    ssh_port,
                )));
            }

            if !next_page {
                break;
            }
            cursor.page += 1;
        }

        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        Ok(())
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

// `GerritSource` reads the parsed configuration fields directly; keep the
// struct field to mirror the baseline without exposing it.
impl GerritSource {
    pub fn config(&self) -> &GerritConnection {
        &self.config
    }

    pub fn ssh(&self) -> bool {
        self.ssh
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
            id: 7,
            kind: crate::KIND_GERRIT.to_owned(),
            display_name: "gerrit".to_owned(),
            config: config.to_owned(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    #[test]
    fn make_repo_decodes_ids_and_builds_clone_urls() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        });
        let source = GerritSource::new(
            service(r#"{"url":"https://gerrit.example.com","username":"u","password":"p"}"#),
            doer,
        )
        .unwrap();
        let project = Project {
            id: "team%2Fproj".to_owned(),
            description: "A project".to_owned(),
            parent: "base".to_owned(),
            ..Project::default()
        };
        let url = Url::parse("https://gerrit.example.com").unwrap();
        let repo = source.make_repo(&project, &url, "gerrit.example.com", 29418);
        assert_eq!(repo.name, "gerrit.example.com/team/proj");
        assert_eq!(repo.uri, "gerrit.example.com/team/proj");
        assert_eq!(repo.external_repo.id, "team%2Fproj");
        assert_eq!(repo.external_repo.service_type, "gerrit");
        assert_eq!(repo.external_repo.service_id, "https://gerrit.example.com/");
        assert!(repo.fork);
        assert!(!repo.private);
        let source_info = repo.sources.get("extsvc:gerrit:7").expect("source");
        assert_eq!(
            source_info.clone_url,
            "https://gerrit.example.com/a/team/proj"
        );
        let metadata = repo.metadata.unwrap();
        assert_eq!(
            metadata["http_url_to_repo"],
            "https://gerrit.example.com/a/team/proj"
        );
        assert_eq!(
            metadata["ssh_url_to_repo"],
            "ssh://gerrit.example.com:29418/team/proj"
        );
    }

    #[test]
    fn authorization_makes_the_source_private() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        });
        let source = GerritSource::new(
            service(
                r#"{"url":"https://gerrit.example.com","username":"u","password":"p","authorization":{}}"#,
            ),
            doer,
        )
        .unwrap();
        let project = Project {
            id: "proj".to_owned(),
            ..Project::default()
        };
        let url = Url::parse("https://gerrit.example.com").unwrap();
        assert!(source.make_repo(&project, &url, "h", 29418).private);
    }

    #[tokio::test]
    async fn list_repos_filters_and_sorts_projects() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![
                crate::HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: b"gerrit.example.com 29418".to_vec(),
                },
                crate::HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: b")]}'\n{\"zzz\":{\"id\":\"zzz\"},\"aaa\":{\"id\":\"aaa\"}}".to_vec(),
                },
                crate::HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: b")]}'\n{}".to_vec(),
                },
                crate::HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: b")]}'\n{}".to_vec(),
                },
            ]),
            requests: Mutex::new(Vec::new()),
        });
        let source = GerritSource::new(
            service(
                r#"{"url":"https://gerrit.example.com","username":"u","password":"p","projects":["aaa"],"exclude":[{"name":"zzz"}]}"#,
            ),
            doer,
        )
        .unwrap();
        let repos = source.list_repos().await;
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].repo.as_ref().unwrap().name, "gerrit.example.com/aaa");
    }
}
