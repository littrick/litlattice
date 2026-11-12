#![forbid(unsafe_code)]

//! `internal/repos/pagure.go`: the repo-updater Pagure source.
//!
//! `ListProjects` is paged 100 at a time; every project becomes a repository
//! named after `path.Join(fullURL.Host, fullURL.Path)`, with `full_url` as the
//! clone URL and the raw project as metadata. Pagure has no exclude config.

use std::sync::Arc;

use async_trait::async_trait;

use crate::azuredevops::parse_urlx;
use crate::github::normalize_base_url;
use crate::gitlab::go_path_join;
use crate::pagure::{Client, ListProjectsArgs, Pagination, Project, TYPE_PAGURE};
use crate::{ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `repos.PagureSource`.
pub struct PagureSource {
    service: SourceService,
    cli: Client,
    service_id: String,
    per_page: i64,
}

impl PagureSource {
    /// `repos.NewPagureSource` with the HTTP transport injected.
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: crate::pagure::PagureConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let cli = Client::new(config, doer)?;
        // `extsvc.NormalizeBaseURL(cli.URL).String()`.
        let service_id = normalize_base_url(&cli.url).to_string();

        Ok(Self {
            service,
            cli,
            service_id,
            per_page: 100,
        })
    }

    pub fn client(&self) -> &Client {
        &self.cli
    }

    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// `PagureSource.makeRepo`.
    pub fn make_repo(&self, project: &Project) -> Result<Repo, String> {
        let urn = self.service.urn();

        let full_url = parse_urlx(&project.full_url)?;
        let host = match full_url.url.port() {
            Some(port) => format!(
                "{}:{}",
                full_url.url.host_str().unwrap_or_default(),
                port
            ),
            None => full_url.url.host_str().unwrap_or_default().to_owned(),
        };
        let name = go_path_join(&host, full_url.url.path());

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url: project.full_url.clone(),
            },
        );

        Ok(Repo {
            name: name.clone(),
            uri: name,
            description: project.description.clone(),
            external_repo: ExternalRepoSpec {
                id: project.id.to_string(),
                service_type: TYPE_PAGURE.to_owned(),
                service_id: self.service_id.clone(),
            },
            sources,
            metadata: Some(
                serde_json::to_value(project).expect("Pagure project metadata serialises"),
            ),
            archived: false,
            fork: project.parent.is_some(),
            stars: 0,
            private: false,
            ..Repo::default()
        })
    }
}

#[async_trait]
impl Source for PagureSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let args = ListProjectsArgs {
            cursor: None,
            tags: self.cli.config.tags.clone(),
            pattern: self.cli.config.pattern.clone(),
            namespace: self.cli.config.namespace.clone(),
            fork: self.cli.config.forks,
        };

        let mut cursor = Some(Pagination {
            per_page: self.per_page,
            page: 1,
            ..Pagination::default()
        });
        let mut results = Vec::new();
        while let Some(current) = cursor.take() {
            match self.cli.list_projects_page(Some(&current), &args).await {
                Ok((projects, next)) => {
                    for project in &projects {
                        match self.make_repo(project) {
                            Ok(repo) => results.push(SourceResult::repo(repo)),
                            Err(err) => {
                                results.push(SourceResult::error(err));
                                return results;
                            }
                        }
                    }
                    cursor = next;
                }
                Err(err) => {
                    results.push(SourceResult::error(err.to_string()));
                    break;
                }
            }
        }
        results
    }

    /// `PagureSource.CheckConnection` assumes availability.
    async fn check_connection(&self) -> Result<(), String> {
        Ok(())
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HttpRequest, HttpResponse};

    struct FixedDoer {
        responses: std::sync::Mutex<Vec<(String, HttpResponse)>>,
    }

    #[async_trait]
    impl HttpDoer for FixedDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            let mut responses = self.responses.lock().unwrap();
            let index = responses
                .iter()
                .position(|(uri, _)| request.url.contains(uri))
                .expect("fixture");
            let (_, response) = responses.remove(index);
            Ok(response)
        }
    }

    fn response(body: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    fn source(config: serde_json::Value, responses: Vec<(String, HttpResponse)>) -> PagureSource {
        let service = SourceService {
            id: 3,
            kind: "PAGURE".to_owned(),
            display_name: "pagure".to_owned(),
            config: serde_json::to_string(&config).unwrap(),
            encryption_key_id: String::new(),
            unrestricted: false,
        };
        PagureSource::new(
            service,
            Arc::new(FixedDoer {
                responses: std::sync::Mutex::new(responses),
            }),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn pages_projects_and_builds_repos() {
        let source = source(
            serde_json::json!({"url": "https://Pagure.Example.com"}),
            vec![(
                "/api/0/projects".to_owned(),
                response(serde_json::json!({
                    "pagination": {"next": "", "page": 1, "per_page": 100},
                    "projects": [{
                        "description": "d",
                        "full_url": "https://pagure.example.com/proj",
                        "fullname": "proj",
                        "id": 9,
                        "name": "proj",
                        "namespace": "ns",
                        "tags": ["a"],
                        "url_path": "proj"
                    }]
                })),
            )],
        );
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        let repo = results[0].repo.as_ref().unwrap();
        assert_eq!(repo.name, "pagure.example.com/proj");
        assert_eq!(repo.external_repo.id, "9");
        assert_eq!(repo.external_repo.service_id, "https://pagure.example.com/");
        assert_eq!(
            repo.sources["extsvc:pagure:3"].clone_url,
            "https://pagure.example.com/proj"
        );
        assert_eq!(repo.metadata.as_ref().unwrap()["tags"], serde_json::json!(["a"]));
    }

    #[test]
    fn make_repo_joins_host_and_path() {
        let source = source(
            serde_json::json!({"url": "https://pagure.example.com"}),
            Vec::new(),
        );
        let repo = source
            .make_repo(&Project {
                full_url: "https://Pagure.Example.com/ns/proj".to_owned(),
                id: 1,
                ..Project::default()
            })
            .unwrap();
        assert_eq!(repo.name, "pagure.example.com/ns/proj");
        assert_eq!(repo.uri, "pagure.example.com/ns/proj");
    }
}
