#![forbid(unsafe_code)]

//! `internal/repos/bitbucketserver.go`: the repo-updater Bitbucket Server
//! source.
//!
//! A sync gathers repositories from the configured `repos` (reverse order, so
//! newly appended entries stream first), `repositoryQuery` strings, and
//! `projectKeys`, deduplicates by repository ID, drops entries whose `state`
//! is not `AVAILABLE`, applies the multi-exact/pattern excluder, and marks
//! repositories carrying the `archived` label. The baseline fans these sources
//! out over goroutines; this port runs them sequentially with the same set
//! semantics.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use serde::Deserialize;
use url::Url;

use crate::auth::{Authenticator, BasicAuth, OAuthBearerToken, OAuthClient};
use crate::bitbucketserver::{BitbucketError, Client, PageToken, Repo, TYPE_BITBUCKETSERVER};
use crate::github::normalize_base_url;
use crate::reposource::bitbucket_server_repo_name;
use crate::{ExternalRepoSpec, HttpDoer, Repo as SourcegraphRepo, Source, SourceInfo, SourceResult, SourceService};

/// `schema.BitbucketServerOAuth`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BitbucketServerOAuth {
    #[serde(default, rename = "consumerKey")]
    pub consumer_key: String,
    #[serde(default, rename = "signingKey")]
    pub signing_key: String,
}

/// `schema.BitbucketServerAuthorization`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BitbucketServerAuthorization {
    #[serde(default)]
    pub oauth: Option<BitbucketServerOAuth>,
    #[serde(default)]
    pub oauth2: bool,
}

/// `schema.ExcludedBitbucketServerRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedBitbucketServerRepo {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pattern: String,
}

/// `schema.BitbucketServerConnection` (the fields the source/client reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BitbucketServerConnection {
    #[serde(default)]
    pub authorization: Option<BitbucketServerAuthorization>,
    #[serde(default)]
    pub exclude: Vec<ExcludedBitbucketServerRepo>,
    #[serde(default, rename = "excludePersonalRepositories")]
    pub exclude_personal_repositories: bool,
    #[serde(default, rename = "gitURLType")]
    pub git_url_type: String,
    #[serde(default)]
    pub password: String,
    #[serde(default, rename = "projectKeys")]
    pub project_keys: Vec<String>,
    #[serde(default)]
    pub repos: Vec<String>,
    #[serde(default, rename = "repositoryPathPattern")]
    pub repository_path_pattern: String,
    #[serde(default, rename = "repositoryQuery")]
    pub repository_query: Vec<String>,
    #[serde(default)]
    pub token: String,
    #[serde(default, rename = "url")]
    pub url: String,
    #[serde(default)]
    pub username: String,
}

/// `repoExcluder` for Bitbucket Server's multi-exact rules.
#[derive(Debug, Default)]
struct Excluder {
    exact_rules: HashSet<String>,
    rules: Vec<ExcludeRule>,
}

#[derive(Debug)]
struct ExcludeRule {
    exact: Vec<String>,
    pattern: Option<Regex>,
}

impl Excluder {
    fn new(entries: &[ExcludedBitbucketServerRepo]) -> Result<Self, String> {
        let mut excluder = Self::default();
        for entry in entries {
            let mut exact = Vec::new();
            if !entry.name.is_empty() {
                exact.push(entry.name.to_lowercase());
            }
            if entry.id != 0 {
                exact.push(entry.id.to_string());
            }
            let pattern = if entry.pattern.is_empty() {
                None
            } else {
                Some(Regex::new(&entry.pattern).map_err(|err| err.to_string())?)
            };

            if !exact.is_empty() && pattern.is_none() {
                excluder.exact_rules.extend(exact);
                continue;
            }
            if exact.is_empty() && pattern.is_none() {
                continue;
            }
            excluder.rules.push(ExcludeRule { exact, pattern });
        }
        Ok(excluder)
    }

    fn should_exclude(&self, input: &str) -> bool {
        let lower = input.to_lowercase();
        if self.exact_rules.contains(&lower) {
            return true;
        }
        self.rules.iter().any(|rule| {
            if !rule.exact.is_empty() && !rule.exact.iter().any(|exact| *exact == lower) {
                return false;
            }
            match &rule.pattern {
                Some(pattern) => pattern.is_match(input),
                None => true,
            }
        })
    }
}

/// `repos.BitbucketServerSource`.
pub struct BitbucketServerSource {
    service: SourceService,
    config: BitbucketServerConnection,
    excluder: Excluder,
    client: Client,
    host: Url,
}

impl BitbucketServerSource {
    /// `repos.NewBitbucketServerSource` with the transport injected.
    ///
    /// Mirrors `bitbucketserver.NewClient`'s authentication selection: OAuth 2
    /// (or no authorization block) uses a bearer token when configured and
    /// basic auth otherwise; a non-OAuth-2 authorization block with an
    /// `oauth` object uses OAuth 1 RSA-SHA1 signing, and with neither leaves
    /// the client unauthenticated.
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: BitbucketServerConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let raw_url = Url::parse(&config.url).map_err(|err| err.to_string())?;
        let host = normalize_base_url(&raw_url);

        let auth: Option<Arc<dyn Authenticator>> = match &config.authorization {
            Some(authorization) if !authorization.oauth2 => match &authorization.oauth {
                Some(oauth) => {
                    let client = OAuthClient::new_rsa_sha1(
                        oauth.consumer_key.clone(),
                        &oauth.signing_key,
                    )
                    .map_err(|err| format!("authorization.oauth.signingKey: {err}"))?;
                    Some(Arc::new(client))
                }
                None => None,
            },
            _ => {
                if !config.token.is_empty() {
                    Some(Arc::new(OAuthBearerToken {
                        token: config.token.clone(),
                        ..OAuthBearerToken::default()
                    }))
                } else {
                    Some(Arc::new(BasicAuth {
                        username: config.username.clone(),
                        password: config.password.clone(),
                    }))
                }
            }
        };

        let excluder = Excluder::new(&config.exclude)?;
        let client = Client::new(&config.url, auth, doer)?;

        Ok(Self {
            service,
            config,
            excluder,
            client,
            host,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// `BitbucketServerSource.makeRepo`.
    pub fn make_repo(&self, repo: &Repo, is_archived: bool) -> SourcegraphRepo {
        let hostname = self.host.host_str().unwrap_or_default().to_owned();
        let project = repo
            .project
            .as_ref()
            .map(|project| project.key.clone())
            .unwrap_or_else(|| "UNKNOWN".to_owned());

        let mut clone_url = String::new();
        for link in repo.links.clone.iter().flatten() {
            if link.name == "ssh" && self.config.git_url_type == "ssh" {
                clone_url = link.href.clone();
                break;
            }
            if link.name == "http" {
                clone_url = link.href.clone();
                if !self.config.username.is_empty() {
                    if let Ok(mut url) = Url::parse(&link.href) {
                        let _ = url.set_username(&self.config.username);
                        clone_url = url.to_string();
                    }
                }
                // No break: an ssh link later in the list still wins.
            }
        }

        let urn = self.service.urn();
        let mut sources = BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url,
            },
        );

        SourcegraphRepo {
            name: bitbucket_server_repo_name(
                &self.config.repository_path_pattern,
                &hostname,
                &project,
                &repo.slug,
            ),
            uri: bitbucket_server_repo_name("", &hostname, &project, &repo.slug),
            description: repo.description.clone(),
            external_repo: ExternalRepoSpec {
                id: repo.id.to_string(),
                service_type: TYPE_BITBUCKETSERVER.to_owned(),
                service_id: self.host.to_string(),
            },
            sources,
            metadata: Some(serde_json::to_value(repo).expect("Bitbucket repo metadata serialises")),
            archived: is_archived,
            fork: repo.origin.is_some(),
            stars: 0,
            private: !repo.public,
            ..SourcegraphRepo::default()
        }
    }

    /// `BitbucketServerSource.excludes`.
    pub fn excludes(&self, repo: &Repo) -> bool {
        let name = match &repo.project {
            Some(project) => format!("{}/{}", project.key, repo.slug),
            None => repo.slug.clone(),
        };
        repo.state != "AVAILABLE"
            || self.excluder.should_exclude(&name)
            || self.excluder.should_exclude(&repo.id.to_string())
            || (self.config.exclude_personal_repositories && repo.is_personal_repository())
    }

    /// `BitbucketServerSource.AuthenticatedUsername`.
    pub async fn authenticated_username(&self) -> Result<String, BitbucketError> {
        self.client.authenticated_username().await
    }

    /// `BitbucketServerSource.Version`.
    pub async fn version(&self) -> Result<String, BitbucketError> {
        self.client.get_version().await
    }

    async fn list_all_labeled_repos(
        &self,
        label: &str,
    ) -> Result<HashSet<i32>, BitbucketError> {
        let mut ids = HashSet::new();
        let mut token = PageToken::first(1000);
        while token.has_more() {
            match self.client.labeled_repos(&token, label).await {
                Ok((repos, next)) => {
                    for repo in repos {
                        ids.insert(repo.id);
                    }
                    token = next;
                }
                Err(err) if err.is_no_such_label() || err.is_not_found() => {
                    return Ok(ids);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(ids)
    }

    fn push_batch(
        &self,
        batch: Vec<Repo>,
        archived: &HashSet<i32>,
        seen: &mut HashSet<i32>,
        results: &mut Vec<SourceResult>,
    ) {
        for repo in batch {
            if seen.contains(&repo.id) || self.excludes(&repo) {
                continue;
            }
            results.push(SourceResult::repo(
                self.make_repo(&repo, archived.contains(&repo.id)),
            ));
            seen.insert(repo.id);
        }
    }
}

#[async_trait]
impl Source for BitbucketServerSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let archived = match self.list_all_labeled_repos("archived").await {
            Ok(archived) => archived,
            Err(err) => {
                return vec![SourceResult::error(format!(
                    "failed to list repos with archived label: {err}"
                ))]
            }
        };

        let mut results = Vec::new();
        let mut seen = HashSet::new();

        for name in self.config.repos.iter().rev() {
            let parts: Vec<&str> = name.splitn(2, '/').collect();
            if parts.len() != 2 {
                results.push(SourceResult::error(format!(
                    "bitbucketserver.repos: name={name:?}"
                )));
                continue;
            }
            match self.client.repo(parts[0], parts[1]).await {
                Ok(repo) => {
                    self.push_batch(vec![repo], &archived, &mut seen, &mut results);
                }
                Err(err) if err.is_not_found() => continue,
                Err(err) => results.push(SourceResult::error(format!(
                    "bitbucketserver.repos: name: {name:?}: {err}"
                ))),
            }
        }

        for query in &self.config.repository_query {
            let query = match query.as_str() {
                "none" => continue,
                "all" => String::new(),
                other => other.to_owned(),
            };
            let mut token = PageToken::first(1000);
            while token.has_more() {
                match self.client.repos(&token, std::slice::from_ref(&query)).await {
                    Ok((repos, next)) => {
                        self.push_batch(repos, &archived, &mut seen, &mut results);
                        token = next;
                    }
                    Err(err) => {
                        results.push(SourceResult::error(format!(
                            "bitbucketserver.repositoryQuery: query={query:?}, page=&{token}: {err}"
                        )));
                        break;
                    }
                }
            }
        }

        for project_key in &self.config.project_keys {
            match self.client.project_repos(project_key).await {
                Ok(repos) => self.push_batch(repos, &archived, &mut seen, &mut results),
                Err(err) => results.push(SourceResult::error(format!(
                    "bitbucketserver.projectKeys: query={project_key:?}: {err}"
                ))),
            }
        }

        results
    }

    /// `BitbucketServerSource.CheckConnection`.
    async fn check_connection(&self) -> Result<(), String> {
        self.authenticated_username().await.map(|_| ()).map_err(|err| {
            format!("connection check failed. could not fetch authenticated user: {err}")
        })
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitbucketserver::{Link, RepoLinks};
    use crate::{HttpRequest, HttpResponse};

    struct FixedDoer {
        responses: std::sync::Mutex<Vec<(&'static str, HttpResponse)>>,
    }

    #[async_trait]
    impl HttpDoer for FixedDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            let mut responses = self.responses.lock().unwrap();
            let index = responses
                .iter()
                .position(|(fragment, _)| request.url.contains(fragment))
                .expect("fixture");
            let (_, response) = responses.remove(index);
            Ok(response)
        }
    }

    fn json_response(body: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    fn service(config: serde_json::Value) -> SourceService {
        SourceService {
            id: 9,
            kind: "BITBUCKETSERVER".to_owned(),
            display_name: "bitbucket".to_owned(),
            config: serde_json::to_string(&config).unwrap(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    fn source(
        config: serde_json::Value,
        responses: Vec<(&'static str, HttpResponse)>,
    ) -> BitbucketServerSource {
        BitbucketServerSource::new(
            service(config),
            Arc::new(FixedDoer {
                responses: std::sync::Mutex::new(responses),
            }),
        )
        .unwrap()
    }

    fn repo(id: i32, slug: &str) -> Repo {
        Repo {
            id,
            slug: slug.to_owned(),
            name: slug.to_owned(),
            state: "AVAILABLE".to_owned(),
            public: true,
            project: Some(crate::bitbucketserver::Project {
                key: "PRJ".to_owned(),
                ..crate::bitbucketserver::Project::default()
            }),
            links: RepoLinks {
                clone: Some(vec![
                    Link {
                        name: "http".to_owned(),
                        href: "https://bitbucket.example.com/scm/prj/repo.git".to_owned(),
                    },
                    Link {
                        name: "ssh".to_owned(),
                        href: "ssh://git@bitbucket.example.com:7999/prj/repo.git".to_owned(),
                    },
                ]),
                ..RepoLinks::default()
            },
            ..Repo::default()
        }
    }

    #[test]
    fn make_repo_uses_ssh_link_and_username_for_http() {
        let source = source(
            serde_json::json!({
                "url": "https://Bitbucket.Example.com/",
                "username": "bot",
                "gitURLType": "http"
            }),
            Vec::new(),
        );
        let mapped = source.make_repo(&repo(1, "repo"), true);
        assert_eq!(mapped.name, "bitbucket.example.com/PRJ/repo");
        assert_eq!(mapped.uri, "bitbucket.example.com/PRJ/repo");
        assert_eq!(mapped.external_repo.id, "1");
        assert_eq!(
            mapped.external_repo.service_id,
            "https://bitbucket.example.com/"
        );
        assert_eq!(
            mapped.sources["extsvc:bitbucketserver:9"].clone_url,
            "https://bot@bitbucket.example.com/scm/prj/repo.git"
        );
        assert!(mapped.archived);
        assert!(!mapped.private);
    }

    #[test]
    fn ssh_git_url_type_prefers_ssh() {
        let source = source(
            serde_json::json!({"url": "https://bitbucket.example.com", "gitURLType": "ssh"}),
            Vec::new(),
        );
        let mapped = source.make_repo(&repo(1, "repo"), false);
        assert_eq!(
            mapped.sources["extsvc:bitbucketserver:9"].clone_url,
            "ssh://git@bitbucket.example.com:7999/prj/repo.git"
        );
    }

    #[test]
    fn excludes_states_ids_and_personal_repos() {
        let source = source(
            serde_json::json!({
                "url": "https://bitbucket.example.com",
                "exclude": [{"id": 42}, {"name": "PRJ/secret"}, {"pattern": "PRJ/tmp.*"}],
                "excludePersonalRepositories": true
            }),
            Vec::new(),
        );
        assert!(source.excludes(&Repo {
            id: 42,
            ..repo(42, "anything")
        }));
        assert!(source.excludes(&Repo {
            slug: "secret".to_owned(),
            ..repo(2, "secret")
        }));
        assert!(source.excludes(&Repo {
            slug: "tmp-x".to_owned(),
            state: "AVAILABLE".to_owned(),
            ..repo(3, "tmp-x")
        }));
        let mut personal = repo(4, "p");
        personal.project.as_mut().unwrap().kind = "PERSONAL".to_owned();
        assert!(source.excludes(&personal));
        let mut unavailable = repo(5, "u");
        unavailable.state = "DISABLED".to_owned();
        assert!(source.excludes(&unavailable));
        assert!(!source.excludes(&repo(6, "public")));
    }

    #[tokio::test]
    async fn list_repos_queries_pages_and_marks_archived() {
        let source = source(
            serde_json::json!({
                "url": "https://bitbucket.example.com",
                "repositoryQuery": ["all"]
            }),
            vec![
                (
                    "/labels/archived/labeled",
                    json_response(serde_json::json!({
                        "size": 1,
                        "limit": 1000,
                        "isLastPage": true,
                        "values": [{"id": 1, "slug": "repo"}]
                    })),
                ),
                (
                    "/rest/api/1.0/repos?",
                    json_response(serde_json::json!({
                        "size": 1,
                        "limit": 1000,
                        "isLastPage": true,
                        "values": [{
                            "id": 1,
                            "slug": "repo",
                            "name": "repo",
                            "state": "AVAILABLE",
                            "public": true,
                            "project": {"key": "PRJ", "type": "NORMAL"},
                            "links": {"clone": [{"name": "http", "href": "https://bitbucket.example.com/scm/prj/repo.git"}]}
                        }]
                    })),
                ),
            ],
        );
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        let mapped = results[0].repo.as_ref().unwrap();
        assert!(mapped.archived);
        assert_eq!(mapped.name, "bitbucket.example.com/PRJ/repo");
    }

    #[tokio::test]
    async fn archived_label_404_is_treated_as_empty() {
        let source = source(
            serde_json::json!({
                "url": "https://bitbucket.example.com",
                "repos": ["PRJ/repo"]
            }),
            vec![
                (
                    "/labels/archived/labeled",
                    HttpResponse {
                        status: 404,
                        headers: Vec::new(),
                        body: br#"{"errors":[{"exceptionName":"com.atlassian.bitbucket.label.NoSuchLabelException"}]}"#.to_vec(),
                    },
                ),
                (
                    "/rest/api/1.0/projects/PRJ/repos/repo",
                    json_response(serde_json::json!({
                        "id": 7,
                        "slug": "repo",
                        "state": "AVAILABLE",
                        "project": {"key": "PRJ"}
                    })),
                ),
            ],
        );
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        assert!(!results[0].repo.as_ref().unwrap().archived);
    }

    #[tokio::test]
    async fn check_connection_reads_the_username_header() {
        let source = source(
            serde_json::json!({"url": "https://bitbucket.example.com"}),
            vec![(
                "/rest/api/1.0/users",
                HttpResponse {
                    status: 200,
                    headers: vec![("X-Ausername".to_owned(), "bot".to_owned())],
                    body: b"{}".to_vec(),
                },
            )],
        );
        assert_eq!(source.check_connection().await, Ok(()));
    }

    #[test]
    fn oauth1_configs_validate_the_signing_key() {
        // `SetOAuth` base64-decodes the signing key and parses a PKCS#1 PEM;
        // an invalid value fails at construction with the baseline's wrap.
        let err = BitbucketServerSource::new(
            service(serde_json::json!({
                "url": "https://bitbucket.example.com",
                "authorization": {"oauth": {"consumerKey": "c", "signingKey": "s"}}
            })),
            Arc::new(FixedDoer {
                responses: std::sync::Mutex::new(Vec::new()),
            }),
        )
        .err()
        .expect("oauth1 rejected");
        assert!(err.starts_with("authorization.oauth.signingKey"), "{err}");
    }
}
