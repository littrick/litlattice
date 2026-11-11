#![forbid(unsafe_code)]

//! `internal/repos/gitlab.go`: the repo-updater GitLab source.
//!
//! The source enumerates explicit `projects` (reverse order, like the
//! baseline's "admins add to the end" hint) and paginates every
//! `projectQuery`, then de-duplicates by project ID and applies exclusions.
//! The five-worker concurrency of the Go implementation is intentionally not
//! reproduced; the result set and error texts are execution-order independent
//! and the differential harness sorts all three streams before comparing.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use url::Url;

use crate::github::normalize_base_url;
use crate::gitlab::{
    is_private_visibility, project_query_to_url, Client, ExcludedGitLabProject, GitLabAuth,
    GitLabConnection, Project, TokenType, TYPE_GITLAB,
};
use crate::reposource::{compile_name_transformations, gitlab_repo_name, NameTransformation};
use crate::{ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// One non-empty exclusion rule, mirroring `repos.rule`.
#[derive(Debug)]
struct GitLabRule {
    exact: Vec<String>,
    patterns: Vec<Regex>,
    /// The `emptyRepos` generic predicate. For string inputs it never
    /// matches; for a [`Project`] it is the only applicable condition.
    empty_repos: bool,
}

impl GitLabRule {
    fn excludes_str(&self, input: &str, lower: &str) -> bool {
        let mut exclude = false;
        if !self.exact.is_empty() {
            exclude = self.exact.iter().any(|exact| exact == lower);
            if !exclude {
                return false;
            }
        }
        for pattern in &self.patterns {
            exclude = pattern.is_match(input);
            if !exclude {
                return false;
            }
        }
        if self.empty_repos {
            // The generic predicate type-asserts `gitlab.Project`; for a
            // string it returns false, so a rule with `emptyRepos` can never
            // exclude a name or a numeric ID.
            exclude = false;
            if !exclude {
                return false;
            }
        }
        exclude
    }

    fn excludes_project(&self, project: &Project) -> bool {
        // `rule.Excludes` skips exacts and patterns for a non-string input and
        // only evaluates the generic predicates.
        self.empty_repos && project.empty_repo
    }
}

/// `repoExcluder` as configured by `GitLabConnection.exclude`.
#[derive(Debug, Default)]
pub struct GitLabExcluder {
    exact_rules: HashSet<String>,
    rules: Vec<GitLabRule>,
}

impl GitLabExcluder {
    pub fn new(rules: &[ExcludedGitLabProject]) -> Result<Self, String> {
        let mut excluder = Self::default();
        for rule in rules {
            let mut exact = Vec::new();
            if !rule.name.is_empty() {
                exact.push(rule.name.to_lowercase());
            }
            if rule.id != 0 {
                exact.push(rule.id.to_string());
            }
            let patterns = if rule.pattern.is_empty() {
                Vec::new()
            } else {
                vec![Regex::new(&rule.pattern).map_err(|err| err.to_string())?]
            };

            if !exact.is_empty() && patterns.is_empty() && !rule.empty_repos {
                excluder.exact_rules.extend(exact);
                continue;
            }
            if exact.is_empty() && patterns.is_empty() && !rule.empty_repos {
                continue;
            }
            excluder.rules.push(GitLabRule {
                exact,
                patterns,
                empty_repos: rule.empty_repos,
            });
        }
        Ok(excluder)
    }

    pub fn should_exclude_str(&self, input: &str) -> bool {
        let lower = input.to_lowercase();
        if self.exact_rules.contains(&lower) {
            return true;
        }
        self.rules
            .iter()
            .any(|rule| rule.excludes_str(input, &lower))
    }

    pub fn should_exclude_project(&self, project: &Project) -> bool {
        self.rules.iter().any(|rule| rule.excludes_project(project))
    }
}

/// `repos.GitLabSource`.
pub struct GitLabSource {
    service: SourceService,
    config: GitLabConnection,
    excluder: GitLabExcluder,
    base_url: Url,
    name_transformations: Vec<NameTransformation>,
    client: Client,
}

impl GitLabSource {
    /// `repos.newGitLabSource` without the HTTP client factory (the transport
    /// is injected as [`HttpDoer`]).
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: GitLabConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let base_url = Url::parse(&config.url).map_err(|err| err.to_string())?;
        let base_url = normalize_base_url(&base_url);

        let excluder = GitLabExcluder::new(&config.exclude)?;

        let transformations: Vec<(String, String)> = config
            .name_transformations
            .iter()
            .map(|transformation| {
                (
                    transformation.regex.clone(),
                    transformation.replacement.clone(),
                )
            })
            .collect();
        let name_transformations = compile_name_transformations(&transformations)?;

        let auth = match TokenType::from_config(&config.token_type) {
            TokenType::OAuth => {
                if config.token.is_empty() {
                    GitLabAuth::None
                } else {
                    GitLabAuth::OAuthBearerToken(config.token.clone())
                }
            }
            TokenType::PersonalAccessToken => {
                if config.token.is_empty() {
                    GitLabAuth::None
                } else {
                    GitLabAuth::PersonalAccessToken(config.token.clone())
                }
            }
        };
        let client = Client::new(&base_url, auth, doer);

        Ok(Self {
            service,
            config,
            excluder,
            base_url,
            name_transformations,
            client,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn excluder(&self) -> &GitLabExcluder {
        &self.excluder
    }

    fn excludes(&self, project: &Project) -> bool {
        self.excluder
            .should_exclude_str(project.path_with_namespace())
            || self
                .excluder
                .should_exclude_str(&project.common.id.to_string())
            || self.excluder.should_exclude_project(project)
    }

    /// `GitLabSource.makeRepo`.
    pub fn make_repo(&self, project: &Project) -> Repo {
        let urn = self.service.urn();
        let hostname = self.base_url.host_str().unwrap_or_default();

        let name = gitlab_repo_name(
            &self.config.repository_path_pattern,
            hostname,
            project.path_with_namespace(),
            &self.name_transformations,
        );
        let uri = gitlab_repo_name(
            "",
            hostname,
            project.path_with_namespace(),
            &self.name_transformations,
        );

        let mut private = is_private_visibility(&project.visibility);
        if project.visibility == crate::gitlab::VISIBILITY_INTERNAL
            && self.config.mark_internal_repos_as_public
        {
            private = false;
        }

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url: self.remote_url(project),
            },
        );

        Repo {
            id: 0,
            deleted: false,
            name,
            uri,
            description: project.common.description.clone(),
            external_repo: ExternalRepoSpec {
                id: project.common.id.to_string(),
                service_type: TYPE_GITLAB.to_owned(),
                service_id: self.base_url.to_string(),
            },
            sources,
            metadata: Some(
                serde_json::to_value(project).expect("GitLab project metadata serialises"),
            ),
            archived: project.archived,
            fork: project.forked_from_project.is_some(),
            stars: project.star_count as i32,
            private,
        }
    }

    /// `GitLabSource.remoteURL`.
    pub fn remote_url(&self, project: &Project) -> String {
        if self.config.git_url_type == "ssh" {
            project.common.ssh_url_to_repo.clone()
        } else {
            project.common.http_url_to_repo.clone()
        }
    }
}

enum ProjectResult {
    Project(Project),
    Error(String),
}

#[async_trait]
impl Source for GitLabSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut unfiltered: Vec<ProjectResult> = Vec::new();

        // Admins append to the end of the list, so the newest entries are
        // streamed first.
        for project in self.config.projects.iter().rev() {
            match self.client.get_project(project.id, &project.name).await {
                Ok(project) => unfiltered.push(ProjectResult::Project(project)),
                Err(err) if err.is_not_found() => {
                    // `skipping missing gitlab.projects entry`
                }
                Err(err) => unfiltered.push(ProjectResult::Error(format!(
                    "gitlab.projects: id: {}, name: {:?}: {}",
                    project.id, project.name, err
                ))),
            }
        }

        for project_query in &self.config.project_query {
            if project_query == "none" {
                continue;
            }

            let mut url_str = match project_query_to_url(project_query, 100) {
                Ok(url) => url,
                Err(err) => {
                    unfiltered.push(ProjectResult::Error(format!(
                        "invalid GitLab projectQuery={:?}: {}",
                        project_query, err
                    )));
                    continue;
                }
            };

            loop {
                match self.client.list_projects(&url_str).await {
                    Ok((projects, next_page_url)) => {
                        unfiltered.extend(projects.into_iter().map(ProjectResult::Project));
                        match next_page_url {
                            Some(next) => url_str = next,
                            None => break,
                        }
                    }
                    Err(err) => {
                        unfiltered.push(ProjectResult::Error(format!(
                            "error listing GitLab projects: url={:?}: {}",
                            url_str, err
                        )));
                        break;
                    }
                }
            }
        }

        let mut seen: HashSet<i64> = HashSet::new();
        let mut results = Vec::with_capacity(unfiltered.len());
        for item in unfiltered {
            match item {
                ProjectResult::Error(err) => results.push(SourceResult::error(err)),
                ProjectResult::Project(project) => {
                    if !seen.contains(&project.common.id) && !self.excludes(&project) {
                        seen.insert(project.common.id);
                        results.push(SourceResult::repo(self.make_repo(&project)));
                    }
                }
            }
        }
        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        self.client.get_user("").await.map(|_| ()).map_err(|err| {
            format!("connection check failed. could not fetch authenticated user: {err}")
        })
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}
