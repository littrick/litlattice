#![forbid(unsafe_code)]

//! `internal/repos/awscodecommit.go`: the repo-updater AWS CodeCommit source.
//!
//! Repositories are listed in `modifiedDate` descending order, 25 metadata
//! entries at a time, named with `repositoryPathPattern` (URI always uses the
//! bare name), and excluded by repository name OR ARN. The external service ID
//! carries the partition/region/account (`arn:<partition>:codecommit:...`).

use std::sync::Arc;

use async_trait::async_trait;

use crate::awscodecommit::{
    resolve_endpoint, service_id, Client, Credentials, Repository, AWSCodeCommitConnection,
};
use crate::exclude::{ExcludeRule, Excluder};
use crate::reposource::aws_repo_name;
use crate::{HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `repos.AWSCodeCommitSource`.
pub struct AWSCodeCommitSource {
    service: SourceService,
    config: AWSCodeCommitConnection,
    excluder: Excluder,
    client: Client,
    aws_partition: &'static str,
    aws_region: String,
}

impl AWSCodeCommitSource {
    /// `repos.NewAWSCodeCommitSource` with the AWS SDK client replaced by the
    /// JSON-protocol client over the injected transport.
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: AWSCodeCommitConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let (aws_partition, endpoint) = resolve_endpoint(&config.region)?;
        let aws_region = config.region.clone();

        let mut rules = Vec::new();
        for excluded in &config.exclude {
            if !excluded.name.is_empty() {
                rules.push(ExcludeRule {
                    name: Some(excluded.name.clone()),
                    pattern: None,
                });
            }
            if !excluded.id.is_empty() {
                rules.push(ExcludeRule {
                    name: Some(excluded.id.clone()),
                    pattern: None,
                });
            }
        }
        let excluder = Excluder::new(&rules)?;

        let credentials = Credentials {
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
            session_token: None,
        };
        let client = Client::new(
            credentials,
            &aws_region,
            aws_partition,
            endpoint,
            doer,
        );

        Ok(Self {
            service,
            config,
            excluder,
            client,
            aws_partition,
            aws_region,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// `AWSCodeCommitSource.makeRepo`.
    pub fn make_repo(&self, repo: &Repository) -> Repo {
        let urn = self.service.urn();
        let service_id = service_id(self.aws_partition, &self.aws_region, &repo.account_id);

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url: repo.http_clone_url.clone(),
            },
        );

        Repo {
            name: aws_repo_name(&self.config.repository_path_pattern, &repo.name),
            uri: aws_repo_name("", &repo.name),
            description: repo.description.clone(),
            external_repo: crate::awscodecommit::external_repo_spec(repo, &service_id),
            sources,
            metadata: Some(serde_json::to_value(repo).expect("AWS repository metadata serialises")),
            archived: false,
            fork: false,
            stars: 0,
            private: !self.service.unrestricted,
            ..Repo::default()
        }
    }

    /// `AWSCodeCommitSource.excludes`: name OR ARN, case-insensitive.
    pub fn excludes(&self, repo: &Repository) -> bool {
        self.excluder.should_exclude(&repo.name) || self.excluder.should_exclude(&repo.id)
    }
}

#[async_trait]
impl Source for AWSCodeCommitSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut next_token = String::new();
        let mut results = Vec::new();
        loop {
            let (batch, token) = match self.client.list_repositories(&next_token).await {
                Ok(page) => page,
                Err(err) => {
                    results.push(SourceResult::error(err.to_string()));
                    return results;
                }
            };

            for repo in &batch {
                if !self.excludes(repo) {
                    results.push(SourceResult::repo(self.make_repo(repo)));
                }
            }

            if batch.is_empty() || token.is_empty() {
                break;
            }
            next_token = token;
        }
        results
    }

    /// `AWSCodeCommitSource.CheckConnection` assumes availability.
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
    use crate::HttpRequest;

    struct FixedDoer {
        responses: std::sync::Mutex<Vec<(&'static str, crate::HttpResponse)>>,
    }

    #[async_trait]
    impl HttpDoer for FixedDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<crate::HttpResponse, String> {
            let target = request
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("X-Amz-Target"))
                .map(|(_, value)| value.clone())
                .unwrap_or_default();
            let operation = target.rsplit('.').next().unwrap_or_default().to_owned();
            let mut responses = self.responses.lock().unwrap();
            let index = responses
                .iter()
                .position(|(name, _)| *name == operation)
                .expect("fixture for operation");
            let (_, response) = responses.remove(index);
            Ok(response)
        }
    }

    fn json_response(body: serde_json::Value) -> crate::HttpResponse {
        crate::HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    fn service(config: serde_json::Value) -> SourceService {
        SourceService {
            id: 5,
            kind: "AWSCODECOMMIT".to_owned(),
            display_name: "aws".to_owned(),
            config: serde_json::to_string(&config).unwrap(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    fn metadata(name: &str, id: &str) -> serde_json::Value {
        serde_json::json!({
            "Arn": format!("arn:aws:codecommit:us-west-2:1:{name}"),
            "accountId": "1",
            "repositoryId": id,
            "repositoryName": name,
            "repositoryDescription": format!("desc {name}"),
            "cloneUrlHttp": format!("https://git-codecommit.us-west-2.amazonaws.com/v1/repos/{name}"),
            "lastModifiedDate": 1577934245
        })
    }

    fn source(
        config: serde_json::Value,
        responses: Vec<(&'static str, crate::HttpResponse)>,
    ) -> AWSCodeCommitSource {
        AWSCodeCommitSource::new(
            service(config),
            Arc::new(FixedDoer {
                responses: std::sync::Mutex::new(responses),
            }),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn lists_names_batches_and_excludes() {
        let source = source(
            serde_json::json!({
                "region": "us-west-2",
                "accessKeyID": "AK",
                "secretAccessKey": "SK",
                "repositoryPathPattern": "aws/{name}",
                "exclude": [{"name": "secret"}]
            }),
            vec![
                (
                    "ListRepositories",
                    json_response(serde_json::json!({
                        "repositories": [
                            {"repositoryName": "repo", "repositoryId": "id-1"},
                            {"repositoryName": "secret", "repositoryId": "id-2"}
                        ],
                        "nextToken": "next"
                    })),
                ),
                (
                    "BatchGetRepositories",
                    json_response(serde_json::json!({
                        "repositories": [metadata("repo", "id-1"), metadata("secret", "id-2")]
                    })),
                ),
                (
                    "ListRepositories",
                    json_response(serde_json::json!({"repositories": []})),
                ),
            ],
        );
        // The fixture for the second page returns no names, so no batch call.
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        let repo = results[0].repo.as_ref().unwrap();
        assert_eq!(repo.name, "aws/repo");
        assert_eq!(repo.uri, "repo");
        assert_eq!(repo.description, "desc repo");
        assert_eq!(repo.external_repo.id, "id-1");
        assert_eq!(
            repo.external_repo.service_id,
            "arn:aws:codecommit:us-west-2:1:"
        );
        assert_eq!(
            repo.sources["extsvc:awscodecommit:5"].clone_url,
            "https://git-codecommit.us-west-2.amazonaws.com/v1/repos/repo"
        );
        assert_eq!(repo.metadata.as_ref().unwrap()["AccountID"], "1");
    }

    #[test]
    fn excludes_by_name_or_id_case_insensitively() {
        let source = source(
            serde_json::json!({
                "region": "us-west-2",
                "exclude": [{"name": "Secret"}, {"id": "id-secret"}]
            }),
            Vec::new(),
        );
        assert!(source.excludes(&Repository {
            name: "secret".to_owned(),
            ..Repository::default()
        }));
        assert!(source.excludes(&Repository {
            id: "ID-SECRET".to_owned(),
            ..Repository::default()
        }));
        assert!(!source.excludes(&Repository {
            name: "public".to_owned(),
            id: "id-public".to_owned(),
            ..Repository::default()
        }));
    }

    #[tokio::test]
    async fn api_errors_are_reported() {
        let source = source(
            serde_json::json!({"region": "us-west-2"}),
            vec![(
                "ListRepositories",
                crate::HttpResponse {
                    status: 400,
                    headers: Vec::new(),
                    body: br#"{"__type":"com.amazonaws.codecommit#RepositoryDoesNotExistException","message":"nope"}"#.to_vec(),
                },
            )],
        );
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].err.as_deref(),
            Some("AWS CodeCommit repository not found")
        );
    }
}
