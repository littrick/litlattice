#![forbid(unsafe_code)]

//! `internal/repos/bitbucketcloud.go`: the repo-updater Bitbucket Cloud
//! source.
//!
//! Teams are paged with `Pagelen: 100`; explicit `repos` entries are fetched
//! newest-first. Repositories whose `scm` is not `git` are dropped, UUIDs are
//! de-duplicated, and exclusions match name/uuid exactly (OR) and the pattern
//! (AND), exactly like `repos.repoExcluder`.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use url::Url;

use crate::bitbucketcloud::{
    encode_go_host, encode_go_path, BitbucketCloudConnection, Client, ExcludedBitbucketCloudRepo,
    PageToken, Repo, TYPE_BITBUCKETCLOUD,
};
use crate::github::normalize_base_url;
use crate::reposource::bitbucket_cloud_repo_name;
use crate::{
    ExternalRepoSpec, HttpDoer, Repo as SourcegraphRepo, Source, SourceInfo, SourceResult,
    SourceService,
};

#[derive(Debug)]
struct BitbucketRule {
    exact: Vec<String>,
    patterns: Vec<Regex>,
}

impl BitbucketRule {
    /// `rule.Excludes` for a string input: each exact is OR'd, then every
    /// pattern must match.
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
        exclude
    }
}

/// `repoExcluder` as configured by `BitbucketCloudConnection.exclude`.
#[derive(Debug, Default)]
pub struct BitbucketCloudExcluder {
    exact_rules: HashSet<String>,
    rules: Vec<BitbucketRule>,
}

impl BitbucketCloudExcluder {
    pub fn new(rules: &[ExcludedBitbucketCloudRepo]) -> Result<Self, String> {
        let mut excluder = Self::default();
        for rule in rules {
            let mut exact = Vec::new();
            if !rule.name.is_empty() {
                exact.push(rule.name.to_lowercase());
            }
            if !rule.uuid.is_empty() {
                exact.push(rule.uuid.to_lowercase());
            }
            let patterns = if rule.pattern.is_empty() {
                Vec::new()
            } else {
                vec![Regex::new(&rule.pattern).map_err(|err| err.to_string())?]
            };

            if !exact.is_empty() && patterns.is_empty() {
                excluder.exact_rules.extend(exact);
                continue;
            }
            if exact.is_empty() && patterns.is_empty() {
                continue;
            }
            excluder.rules.push(BitbucketRule { exact, patterns });
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
}

/// `repos.BitbucketCloudSource`.
pub struct BitbucketCloudSource {
    service: SourceService,
    config: BitbucketCloudConnection,
    excluder: BitbucketCloudExcluder,
    /// The normalized web URL (`config.Url`), used for repo names and the
    /// external service ID.
    host: Url,
    client: Client,
}

impl BitbucketCloudSource {
    /// `repos.newBitbucketCloudSource` without the HTTP client factory.
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: BitbucketCloudConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let excluder = BitbucketCloudExcluder::new(&config.exclude)?;
        let client = Client::new(&config, doer)?;
        // The baseline parses `config.Url` lazily in `makeRepo` and panics on
        // failure; parsing here turns that impossible configuration into a
        // construction error instead.
        let host = Url::parse(&config.url).map_err(|err| err.to_string())?;
        let host = normalize_base_url(&host);

        Ok(Self {
            service,
            config,
            excluder,
            host,
            client,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn excluder(&self) -> &BitbucketCloudExcluder {
        &self.excluder
    }

    fn excludes(&self, repo: &Repo) -> bool {
        self.excluder.should_exclude_str(&repo.full_name)
            || self.excluder.should_exclude_str(&repo.uuid)
    }

    /// `BitbucketCloudSource.makeRepo`.
    pub fn make_repo(&self, repo: &Repo) -> SourcegraphRepo {
        let urn = self.service.urn();
        let hostname = self.host.host_str().unwrap_or_default();

        let name = bitbucket_cloud_repo_name(
            &self.config.repository_path_pattern,
            hostname,
            &repo.full_name,
        );
        let uri = bitbucket_cloud_repo_name("", hostname, &repo.full_name);

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url: self.remote_url(repo),
            },
        );

        SourcegraphRepo {
            id: 0,
            deleted: false,
            name,
            uri,
            description: repo.description.clone(),
            external_repo: ExternalRepoSpec {
                id: repo.uuid.clone(),
                service_type: TYPE_BITBUCKETCLOUD.to_owned(),
                service_id: self.host.to_string(),
            },
            sources,
            metadata: Some(serde_json::to_value(repo).expect("Bitbucket repo metadata serialises")),
            archived: false,
            fork: repo.parent.is_some(),
            stars: 0,
            private: repo.is_private,
        }
    }

    /// `BitbucketCloudSource.remoteURL`.
    pub fn remote_url(&self, repo: &Repo) -> String {
        if self.config.git_url_type == "ssh" {
            return format!("git@{}:{}.git", self.config.url, repo.full_name);
        }

        // `(&url.URL{Scheme: "https", Host: config.Url, Path: "/" + fullName}).String()`.
        let fallback = format!(
            "https://{}{}",
            encode_go_host(&self.config.url),
            encode_go_path(&format!("/{}", repo.full_name))
        );
        repo.links.https().map(str::to_owned).unwrap_or(fallback)
    }
}

enum RepoResult {
    Repo(Repo),
    Error(String),
}

#[async_trait]
impl Source for BitbucketCloudSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut unfiltered: Vec<RepoResult> = Vec::new();

        // List all repositories of the selected teams.
        for team in &self.config.teams {
            let mut page = PageToken {
                pagelen: 100,
                ..PageToken::default()
            };
            loop {
                // `page.HasMore() || page.Page == 0`
                if !(page.has_more() || page.page == 0) {
                    break;
                }
                match self.client.repos(Some(&page), team, None).await {
                    Ok((repos, next)) => {
                        page = next;
                        unfiltered.extend(repos.into_iter().map(RepoResult::Repo));
                    }
                    Err(err) => {
                        unfiltered.push(RepoResult::Error(format!(
                            "bitbucketcloud.teams: item={:?}, page=<nil>: {}",
                            team, err
                        )));
                        break;
                    }
                }
            }
        }

        // Explicit repositories, newest first.
        for name in self.config.repos.iter().rev() {
            let parts: Vec<&str> = name.splitn(2, '/').collect();
            if parts.len() != 2 {
                unfiltered.push(RepoResult::Error(format!(
                    "invalid repo name, expected format <workspace>/<repo_slug>, got {:?}",
                    name
                )));
                continue;
            }
            match self.client.repo(parts[0], parts[1]).await {
                Ok(repo) => unfiltered.push(RepoResult::Repo(repo)),
                Err(err) if err.is_not_found() => {
                    // `skipping missing bitbucketcloud.repos entry`
                }
                Err(err) => unfiltered.push(RepoResult::Error(format!(
                    "failed to fetch repo {:?}: {}",
                    name, err
                ))),
            }
        }

        let mut seen: HashSet<String> = HashSet::new();
        let mut results = Vec::with_capacity(unfiltered.len());
        for item in unfiltered {
            match item {
                RepoResult::Error(err) => results.push(SourceResult::error(err)),
                RepoResult::Repo(repo) => {
                    // Discard non-Git repositories.
                    if repo.scm != "git" {
                        continue;
                    }
                    if !seen.contains(&repo.uuid) && !self.excludes(&repo) {
                        seen.insert(repo.uuid.clone());
                        results.push(SourceResult::repo(self.make_repo(&repo)));
                    }
                }
            }
        }
        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        self.client
            .repos(None, "", None)
            .await
            .map(|_| ())
            .map_err(|err| {
                format!("connection check failed. could not fetch authenticated user: {err}")
            })
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}
