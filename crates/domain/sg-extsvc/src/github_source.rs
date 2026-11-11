#![forbid(unsafe_code)]

//! `internal/repos/github.go`: the GitHub external service source.
//!
//! This is a port of the repository enumeration, rename and removal semantics
//! the repo-updater syncer relies on: `ListRepos` unions `repos`,
//! `repositoryQuery`, `orgs` and (for GitHub Apps) the installation, dedupes
//! by the GitHub database ID, applies the configured exclusions, and maps each
//! repository to the `types.Repo` shape (`makeRepo`). Renames surface as a
//! changed `Name` and removals as a repository the source no longer yields;
//! the syncer owns both effects. GitHub App authentication is not ported, so
//! connections with `gitHubAppDetails` fail construction explicitly.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use url::Url;

use crate::auth::{Authenticator, OAuthBearerToken};
use crate::github::{
    api_root, canonicalized_url, is_not_found, normalize_base_url, repository_metadata,
    split_repository_name_with_owner, GithubError, Repository, SearchReposParams, V3Client,
    V4Client, DATE_RANGE_FORMAT, TYPE_GITHUB, VISIBILITY_ALL, VISIBILITY_INTERNAL,
};
use crate::reposource::github_repo_name;
use crate::{ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `schema.GitHubConnection` (the fields this slice consumes).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitHubConnection {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub repos: Vec<String>,
    #[serde(default)]
    pub orgs: Vec<String>,
    #[serde(default, rename = "repositoryQuery")]
    pub repository_query: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<ExcludedGitHubRepo>,
    #[serde(default, rename = "gitURLType")]
    pub git_url_type: String,
    #[serde(default, rename = "repositoryPathPattern")]
    pub repository_path_pattern: String,
    #[serde(default, rename = "gitHubAppDetails")]
    pub git_hub_app_details: Option<GitHubAppDetails>,
    #[serde(default)]
    pub authorization: Option<GitHubAuthorization>,
}

/// `schema.GitHubAppDetails`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitHubAppDetails {
    #[serde(default, rename = "appID")]
    pub app_id: i32,
    #[serde(default, rename = "baseURL")]
    pub base_url: String,
    #[serde(default, rename = "cloneAllRepositories")]
    pub clone_all_repositories: bool,
    #[serde(default, rename = "installationID")]
    pub installation_id: i32,
}

/// `schema.GitHubAuthorization`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitHubAuthorization {
    #[serde(default, rename = "markInternalReposAsPublic")]
    pub mark_internal_repos_as_public: bool,
}

/// `schema.ExcludedGitHubRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedGitHubRepo {
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub forks: bool,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub stars: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operator {
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl Operator {
    fn parse(input: &str) -> Result<Self, String> {
        match input {
            "<" => Ok(Self::Less),
            "<=" => Ok(Self::LessOrEqual),
            ">" => Ok(Self::Greater),
            ">=" => Ok(Self::GreaterOrEqual),
            _ => Err(format!("invalid operator {input:?}")),
        }
    }

    fn eval(self, left: i64, right: i64) -> bool {
        match self {
            Self::Less => left < right,
            Self::LessOrEqual => left <= right,
            Self::Greater => left > right,
            Self::GreaterOrEqual => left >= right,
        }
    }
}

#[derive(Debug)]
enum Predicate {
    Size(Operator, i64),
    Stars(Operator, i64),
    Archived,
    Forks,
}

impl Predicate {
    fn matches(&self, repo: &Repository) -> bool {
        match self {
            Predicate::Size(operator, size) => {
                let repo_size = repo.size_bytes();
                // If we don't have a repository size, we don't exclude.
                repo_size != 0 && operator.eval(repo_size, *size)
            }
            Predicate::Stars(operator, count) => {
                operator.eval(i64::from(repo.stargazer_count), *count)
            }
            Predicate::Archived => repo.is_archived,
            Predicate::Forks => repo.is_fork,
        }
    }
}

#[derive(Debug)]
struct GitHubRule {
    exact: Vec<String>,
    patterns: Vec<Regex>,
    predicates: Vec<Predicate>,
}

impl GitHubRule {
    /// `rule.Excludes(string)`.
    fn excludes_str(&self, input: &str) -> bool {
        let lower = input.to_lowercase();
        let mut exclude = false;
        if !self.exact.is_empty() {
            exclude = self.exact.iter().any(|exact| *exact == lower);
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
        // Generic predicates type-assert to `github.Repository`, so a string
        // input never matches them (`rule.Excludes`).
        exclude && self.predicates.is_empty()
    }

    /// `rule.Excludes(github.Repository)`: only generic predicates apply.
    fn excludes_repo(&self, repo: &Repository) -> bool {
        if self.predicates.is_empty() {
            return false;
        }
        self.predicates
            .iter()
            .all(|predicate| predicate.matches(repo))
    }
}

/// `repos.repoExcluder` for GitHub rules.
#[derive(Debug, Default)]
pub struct GitHubExcluder {
    exact_rules: HashSet<String>,
    rules: Vec<GitHubRule>,
}

impl GitHubExcluder {
    pub fn new(rules: &[ExcludedGitHubRepo]) -> Result<Self, String> {
        let mut excluder = Self::default();
        for rule in rules {
            let mut exact = Vec::new();
            if !rule.name.is_empty() {
                exact.push(rule.name.to_lowercase());
            }
            if !rule.id.is_empty() {
                exact.push(rule.id.to_lowercase());
            }
            let patterns = if rule.pattern.is_empty() {
                Vec::new()
            } else {
                vec![Regex::new(&rule.pattern).map_err(|err| err.to_string())?]
            };
            let mut predicates = Vec::new();
            if !rule.size.is_empty() {
                predicates.push(parse_size_constraint(&rule.size)?);
            }
            if !rule.stars.is_empty() {
                predicates.push(parse_stars_constraint(&rule.stars)?);
            }
            if rule.archived {
                predicates.push(Predicate::Archived);
            }
            if rule.forks {
                predicates.push(Predicate::Forks);
            }
            if !exact.is_empty() && patterns.is_empty() && predicates.is_empty() {
                excluder.exact_rules.extend(exact);
                continue;
            }
            if exact.is_empty() && patterns.is_empty() && predicates.is_empty() {
                continue;
            }
            excluder.rules.push(GitHubRule {
                exact,
                patterns,
                predicates,
            });
        }
        Ok(excluder)
    }

    pub fn should_exclude_str(&self, input: &str) -> bool {
        let lower = input.to_lowercase();
        if self.exact_rules.contains(&lower) {
            return true;
        }
        self.rules.iter().any(|rule| rule.excludes_str(input))
    }

    pub fn should_exclude_repo(&self, repo: &Repository) -> bool {
        self.rules.iter().any(|rule| rule.excludes_repo(repo))
    }

    /// `GitHubSource.excludes`.
    pub fn excludes(&self, repo: &Repository) -> bool {
        repo.is_locked
            || repo.is_disabled
            || self.should_exclude_str(&repo.name_with_owner)
            || self.should_exclude_str(&repo.id)
            || self.should_exclude_repo(repo)
    }
}

fn parse_size_constraint(constraint: &str) -> Result<Predicate, String> {
    let regex = Regex::new(r"([<>=]{1,2})\s*(\d+\s*\w+)").expect("static regex");
    let captures = regex
        .captures(constraint)
        .ok_or_else(|| format!("invalid size constraint format: {constraint:?}"))?;
    let operator = Operator::parse(&captures[1])
        .map_err(|err| format!("failed to evaluate size constraint: {err}"))?;
    let size = parse_bytesize(&captures[2])
        .map_err(|err| format!("failed to evaluate size constraint: {err}"))?;
    Ok(Predicate::Size(operator, size))
}

fn parse_stars_constraint(constraint: &str) -> Result<Predicate, String> {
    let regex = Regex::new(r"([<>=]{1,2})\s*(\d+)").expect("static regex");
    let captures = regex
        .captures(constraint)
        .ok_or_else(|| format!("invalid stars constraint format: {constraint:?}"))?;
    let operator = Operator::parse(&captures[1])
        .map_err(|err| format!("failed to evaluate stars constraint: {err}"))?;
    let count: i64 = captures[2]
        .parse()
        .map_err(|err| format!("failed to evaluate stars constraint: {err}"))?;
    Ok(Predicate::Stars(operator, count))
}

/// `bytesize.Parse`.
fn parse_bytesize(input: &str) -> Result<i64, String> {
    let input = input.trim();
    let digit_end = input
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(index, _)| index)
        .unwrap_or(input.len());
    if digit_end == 0 {
        return Err(format!("missing number at start of string: {input}"));
    }
    let number: i64 = input[..digit_end].parse().map_err(|err| {
        format!(
            "failed to parse {:?} into number: {err}",
            &input[..digit_end]
        )
    })?;
    let unit = match input[digit_end..].trim() {
        "B" | "b" => 1,
        "kB" | "KB" => 1_000,
        "kiB" | "KiB" => 1_024,
        "MiB" => 1_024 * 1_024,
        "MB" => 1_000 * 1_000,
        "GiB" => 1_024 * 1_024 * 1_024,
        "GB" => 1_000 * 1_000 * 1_000,
        other => return Err(format!("unknown unit: {other}")),
    };
    number
        .checked_mul(unit)
        .filter(|value| *value >= 0)
        .ok_or_else(|| "value overflows max size".to_owned())
}

/// `repos.GitHubSource`.
pub struct GitHubSource {
    service: SourceService,
    config: GitHubConnection,
    excluder: GitHubExcluder,
    github_dot_com: bool,
    base_url: Url,
    original_hostname: String,
    v3: V3Client,
    v4: V4Client,
    mark_internal_repos_as_public: bool,
}

#[derive(Debug, Clone)]
struct RepoResult {
    repo: Option<Repository>,
    err: Option<String>,
}

impl RepoResult {
    fn repo(repo: Repository) -> Self {
        Self {
            repo: Some(repo),
            err: None,
        }
    }

    fn error(err: impl Into<String>) -> Self {
        Self {
            repo: None,
            err: Some(err.into()),
        }
    }
}

impl GitHubSource {
    /// `repos.NewGitHubSource` given the already-decrypted configuration.
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: GitHubConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;
        if config.git_hub_app_details.is_some() {
            return Err(
                "GitHub App authentication is not ported (gitHubAppDetails is unsupported)"
                    .to_owned(),
            );
        }

        let base_url = Url::parse(&config.url).map_err(|err| err.to_string())?;
        let base_url = normalize_base_url(&base_url);
        let original_hostname = base_url.host_str().unwrap_or_default().to_owned();
        let (api_url, github_dot_com) = api_root(&base_url);

        let auth: Arc<dyn Authenticator> = Arc::new(OAuthBearerToken {
            token: config.token.clone(),
            ..OAuthBearerToken::default()
        });

        let excluder = GitHubExcluder::new(&config.exclude)?;
        let mark_internal_repos_as_public = config
            .authorization
            .as_ref()
            .is_some_and(|authorization| authorization.mark_internal_repos_as_public);

        let v3 = V3Client::new(
            canonicalized_url(&api_url),
            Some(Arc::clone(&auth)),
            Arc::clone(&doer),
        );
        let v4 = V4Client::new(canonicalized_url(&api_url), Some(auth), doer);

        Ok(Self {
            service,
            config,
            excluder,
            github_dot_com,
            base_url,
            original_hostname,
            v3,
            v4,
            mark_internal_repos_as_public,
        })
    }

    /// `GitHubSource.ExternalServices`.
    pub fn external_service(&self) -> SourceService {
        self.service.clone()
    }

    /// `GitHubSource.Version`.
    pub async fn version(&self) -> Result<String, String> {
        self.v3.get_version().await.map_err(|err| err.to_string())
    }

    /// `GitHubSource.ValidateAuthenticator`.
    pub async fn validate_authenticator(&self) -> Result<(), String> {
        self.v3
            .get_authenticated_oauth_scopes()
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn collect_unfiltered(&self, results: &mut Vec<RepoResult>) {
        self.list_repos_config(results).await;

        // Admins normally add to the end of lists, so the end of the list
        // most likely has new repos; stream them first.
        for query in self.config.repository_query.iter().rev() {
            self.list_repository_query(query, results).await;
        }
        for org in self.config.orgs.iter().rev() {
            self.list_org(org, results).await;
        }
        // `gitHubAppDetails` is rejected at construction.
    }

    async fn list_repos_config(&self, results: &mut Vec<RepoResult>) {
        if self.config.repos.is_empty() {
            return;
        }
        if self
            .fetch_all_repositories_in_batches(results)
            .await
            .is_ok()
        {
            return;
        }

        // Fall back to the slower sequential fetch if the batch GraphQL call
        // failed, exactly like the baseline.
        for name_with_owner in self.config.repos.iter().rev() {
            let (owner, name) = match split_repository_name_with_owner(name_with_owner) {
                Ok(parts) => parts,
                Err(_) => {
                    results.push(RepoResult::error(format!(
                        "Invalid GitHub repository: nameWithOwner={name_with_owner}"
                    )));
                    return;
                }
            };
            match self.v3.get_repository(&owner, &name).await {
                Ok(repo) => results.push(RepoResult::repo(repo)),
                Err(err) if is_not_found(&err) => {
                    // Missing entries are skipped with a warning in the baseline.
                }
                Err(err) => results.push(RepoResult::error(format!(
                    "Error getting GitHub repository: nameWithOwner={name_with_owner}: {err}"
                ))),
            }
        }
    }

    async fn fetch_all_repositories_in_batches(
        &self,
        results: &mut Vec<RepoResult>,
    ) -> Result<(), String> {
        const BATCH_SIZE: usize = 30;
        let mut end = self.config.repos.len();
        while end > 0 {
            let start = end.saturating_sub(BATCH_SIZE);
            let batch = &self.config.repos[start..end];
            let repos = self
                .v4
                .get_repos_by_name_with_owner(batch)
                .await
                .map_err(|err| format!("GetReposByNameWithOwner failed: {err}"))?;
            for repo in repos {
                results.push(RepoResult::repo(repo));
            }
            end = start;
        }
        Ok(())
    }

    async fn list_repository_query(&self, query: &str, results: &mut Vec<RepoResult>) {
        match query {
            "public" => self.list_public(results).await,
            "affiliated" => self.list_affiliated(results).await,
            "internal" => self.list_internal(results).await,
            "none" => {}
            other => {
                if let Some(org) = match_org(other) {
                    self.list_org(&org, results).await;
                } else {
                    self.list_search(other, results).await;
                }
            }
        }
    }

    async fn list_org(&self, org: &str, results: &mut Vec<RepoResult>) {
        // The GitHub API does not return internal repositories with
        // `type=all`, so the baseline queries "all" and "internal" and
        // dedupes by the GraphQL repository ID.
        let mut seen: HashSet<String> = HashSet::new();
        if let Some(err) = self.paginate_org(org, "all", &mut seen, results).await {
            if crate::github::http_error_code(&err) == 404 {
                // Handle 404 from the org repos endpoint by trying the user
                // repos endpoint. Only if that also fails is the original
                // "organisation not found" reported.
                if self.paginate_user(org, &mut seen, results).await.is_some() {
                    results.push(RepoResult::error(format!(
                        "organisation {org:?} (specified in configuration) not found"
                    )));
                }
                return;
            }
            // Any other first-page error is still reported, and the baseline
            // goes on to query the internal repositories.
            results.push(RepoResult::error(err.to_string()));
        }

        if let Some(err) = self.paginate_org(org, "internal", &mut seen, results).await {
            if crate::github::http_error_code(&err) == 404 {
                results.push(RepoResult::error(format!(
                    "organisation {org:?} (specified in configuration) not found"
                )));
            } else {
                results.push(RepoResult::error(err.to_string()));
            }
        }
    }

    /// The `paginateRepos` shape used by `listOrg`: successful pages are
    /// emitted immediately (deduped by the GraphQL repository ID), a
    /// first-page error is returned, and later-page errors are emitted as a
    /// result. `getReposByType`'s deferred 404 rewrite is applied by the
    /// caller.
    async fn paginate_org(
        &self,
        org: &str,
        repo_type: &str,
        seen: &mut HashSet<String>,
        results: &mut Vec<RepoResult>,
    ) -> Option<GithubError> {
        let mut page = 1;
        loop {
            match self.v3.list_org_repositories(org, page, repo_type).await {
                Ok((repos, has_next)) => {
                    push_unique(results, seen, repos);
                    if !has_next {
                        return None;
                    }
                }
                Err(err) => {
                    if page == 1 {
                        return Some(err);
                    }
                    results.push(RepoResult::error(err.to_string()));
                    return None;
                }
            }
            page += 1;
        }
    }

    /// `listUser`: a first-page error is captured and reported by the caller
    /// as the original org error; later-page errors are emitted as results.
    async fn paginate_user(
        &self,
        user: &str,
        seen: &mut HashSet<String>,
        results: &mut Vec<RepoResult>,
    ) -> Option<GithubError> {
        let mut page = 1;
        loop {
            match self.v3.list_user_repositories(user, page).await {
                Ok((repos, has_next)) => {
                    push_unique(results, seen, repos);
                    if !has_next {
                        return None;
                    }
                }
                Err(err) => {
                    if page == 1 {
                        return Some(err);
                    }
                    results.push(RepoResult::error(err.to_string()));
                    return None;
                }
            }
            page += 1;
        }
    }

    async fn list_affiliated(&self, results: &mut Vec<RepoResult>) {
        let mut page = 1;
        loop {
            match self
                .v3
                .list_affiliated_repositories(VISIBILITY_ALL, page, 100)
                .await
            {
                Ok((repos, has_next)) => {
                    results.extend(repos.into_iter().map(RepoResult::repo));
                    if !has_next {
                        return;
                    }
                }
                Err(err) => {
                    results.push(RepoResult::error(err.to_string()));
                    return;
                }
            }
            page += 1;
        }
    }

    async fn list_public(&self, results: &mut Vec<RepoResult>) {
        if self.github_dot_com {
            results.push(RepoResult::error(
                "unsupported configuration \"public\" for \"repositoryQuery\" for github.com",
            ));
            return;
        }
        let mut since_repo_id = 0i64;
        loop {
            let (repos, has_next) = match self.v3.list_public_repositories(since_repo_id).await {
                Ok(result) => result,
                Err(err) if crate::github::http_error_code(&err) == 404 => return,
                Err(err) => {
                    results.push(RepoResult::error(format!(
                        "failed to list public repositories: sinceRepoID={since_repo_id}: {err}"
                    )));
                    return;
                }
            };
            for batch in repos.chunks(30) {
                let names: Vec<String> = batch
                    .iter()
                    .map(|repo| repo.name_with_owner.clone())
                    .collect();
                match self.v4.get_repos_by_name_with_owner(&names).await {
                    Ok(full_repos) => {
                        results.extend(full_repos.into_iter().map(RepoResult::repo));
                    }
                    Err(err) => {
                        results.push(RepoResult::error(format!(
                            "failed to get full information for public repositories: sinceRepoID={since_repo_id}: {err}"
                        )));
                        return;
                    }
                }
            }
            let Some(last) = repos.last() else {
                return;
            };
            since_repo_id = last.database_id;
            if !has_next {
                return;
            }
        }
    }

    async fn list_internal(&self, results: &mut Vec<RepoResult>) {
        let mut orgs = Vec::new();
        let mut page = 1;
        loop {
            match self.v3.get_authenticated_user_orgs(page).await {
                Ok((page_orgs, has_next)) => {
                    orgs.extend(page_orgs);
                    if !has_next {
                        break;
                    }
                }
                Err(err) => {
                    results.push(RepoResult::error(err.to_string()));
                    return;
                }
            }
            page += 1;
        }
        for org in orgs {
            let mut page = 1;
            loop {
                match self
                    .v3
                    .list_org_repositories(&org.login, page, VISIBILITY_INTERNAL)
                    .await
                {
                    Ok((repos, has_next)) => {
                        results.extend(repos.into_iter().map(RepoResult::repo));
                        if !has_next {
                            break;
                        }
                    }
                    Err(err) => {
                        results.push(RepoResult::error(err.to_string()));
                        break;
                    }
                }
                page += 1;
            }
        }
    }

    async fn list_search(&self, query: &str, results: &mut Vec<RepoResult>) {
        let repository_query = RepositoryQuery::new(query, OffsetDateTime::now_utc());
        self.do_with_refined_window(repository_query, results).await;
    }

    async fn do_with_refined_window(
        &self,
        mut query: RepositoryQuery,
        results: &mut Vec<RepoResult>,
    ) {
        if query.first == 0 {
            query.first = 100;
        }
        if query.limit == 0 {
            query.limit = 1000;
        }
        if let Err(err) = self.do_recursively(&mut query, results).await {
            results.push(RepoResult::error(format!(
                "failed to search GitHub repositories with {:?}: {err}",
                query.string()
            )));
        }
    }

    fn split<'a>(
        &'a self,
        query: &'a mut RepositoryQuery,
        results: &'a mut Vec<RepoResult>,
    ) -> Pin<Box<dyn Future<Output = Result<(), GithubError>> + Send + 'a>> {
        Box::pin(async move {
            let original_count = query.count;
            let middle = query.created_from + (query.created_to - query.created_from) / 2;
            let mut q1 = query.clone();
            q1.known = false;
            q1.created_from = query.created_from;
            q1.created_to = middle - time::Duration::seconds(1);
            let mut q2 = query.clone();
            q2.created_from = middle;
            q2.created_to = query.created_to;
            self.do_recursively(&mut q1, results).await?;
            // We now know q2's count by subtracting q1's (updated count) from
            // the original.
            q2.known = true;
            q2.count = original_count - q1.count;
            self.do_recursively(&mut q2, results).await
        })
    }

    fn do_recursively<'a>(
        &'a self,
        query: &'a mut RepositoryQuery,
        results: &'a mut Vec<RepoResult>,
    ) -> Pin<Box<dyn Future<Output = Result<(), GithubError>> + Send + 'a>> {
        Box::pin(async move {
            if query.known
                && query.count > query.limit
                && (query.created_to - query.created_from) >= time::Duration::seconds(2)
            {
                return self.split(query, results).await;
            }

            let mut response = match self.search_repos(query).await {
                Ok(response) => response,
                // The baseline returns nil here; the caller's error path only
                // covers split/recursion errors.
                Err(_) => return Ok(()),
            };
            query.known = true;
            query.count = response.1;

            if query.count > query.limit
                && (query.created_to - query.created_from) >= time::Duration::seconds(2)
            {
                return self.split(query, results).await;
            }

            const MAX_TRIES: i32 = 3;
            let mut num_tries = 0;
            let mut seen: HashSet<i64> = HashSet::new();
            loop {
                for repo in &response.0 {
                    if seen.insert(repo.database_id) {
                        results.push(RepoResult::repo(repo.clone()));
                        if seen.len() as i32 >= response.1 {
                            break;
                        }
                    }
                }
                if seen.len() as i32 >= response.1 || seen.len() as i32 >= query.limit {
                    break;
                }
                if response.2.is_empty() {
                    num_tries += 1;
                    if num_tries >= MAX_TRIES {
                        break;
                    }
                }
                // The baseline retries with the previous response's cursor.
                query.cursor = response.2.clone();
                response = self.search_repos(query).await?;
            }
            Ok(())
        })
    }

    /// `V4Client.SearchRepos` for a `repositoryQuery`, returning
    /// `(repos, totalCount, endCursor)`.
    async fn search_repos(
        &self,
        query: &RepositoryQuery,
    ) -> Result<(Vec<Repository>, i32, String), GithubError> {
        let result = self
            .v4
            .search_repos(SearchReposParams {
                query: query.string(),
                first: query.first,
                after: query.cursor.clone(),
            })
            .await?;
        Ok((result.repos, result.total_count, result.end_cursor))
    }

    /// `GitHubSource.makeRepo`.
    pub fn make_repo(&self, repo: &Repository) -> Repo {
        let mut metadata_repo = repo.clone();
        // This field flips depending on which token retrieved the repo, so
        // it is not stored.
        metadata_repo.viewer_permission.clear();
        metadata_repo.description = sanitize_to_utf8(&metadata_repo.description);
        metadata_repo.visibility = metadata_repo.visibility.to_lowercase();

        let is_private =
            if metadata_repo.visibility == "internal" && self.mark_internal_repos_as_public {
                false
            } else {
                repo.is_private
            };

        let urn = self.service.urn();
        let clone_url = self.remote_url(repo);
        let service_id = normalize_base_url(&self.base_url).to_string();
        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url,
            },
        );

        Repo {
            name: github_repo_name(
                &self.config.repository_path_pattern,
                &self.original_hostname,
                &repo.name_with_owner,
            ),
            uri: github_repo_name("", &self.original_hostname, &repo.name_with_owner),
            external_repo: ExternalRepoSpec {
                id: repo.id.clone(),
                service_type: TYPE_GITHUB.to_owned(),
                service_id,
            },
            description: sanitize_to_utf8(&repo.description),
            fork: repo.is_fork,
            archived: repo.is_archived,
            stars: repo.stargazer_count,
            private: is_private,
            sources,
            metadata: Some(repository_metadata(&metadata_repo)),
            ..Repo::default()
        }
    }

    /// `GitHubSource.remoteURL`.
    pub fn remote_url(&self, repo: &Repository) -> String {
        if self.config.git_url_type == "ssh" {
            return format!(
                "git@{}:{}.git",
                self.original_hostname, repo.name_with_owner
            );
        }
        repo.url.clone()
    }

    /// `GitHubSource.excludes`.
    pub fn excludes(&self, repo: &Repository) -> bool {
        self.excluder.excludes(repo)
    }
}

fn push_unique(results: &mut Vec<RepoResult>, seen: &mut HashSet<String>, repos: Vec<Repository>) {
    for repo in repos {
        if seen.insert(repo.id.clone()) {
            results.push(RepoResult::repo(repo));
        }
    }
}

fn sanitize_to_utf8(value: &str) -> String {
    value.replace('\0', "")
}

/// `repos.matchOrg`'s `regOrg`.
fn match_org(query: &str) -> Option<String> {
    let regex = Regex::new(r"^org:([a-zA-Z0-9](?:-?[a-zA-Z0-9]){0,38})$").expect("static regex");
    regex.captures(query).map(|captures| captures[1].to_owned())
}

/// `repos.dateRange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateRange {
    from: OffsetDateTime,
    to: OffsetDateTime,
}

impl DateRange {
    /// `dateRange.String`.
    fn string(self) -> String {
        format!(
            "{}..{}",
            self.from.format(DATE_RANGE_FORMAT).unwrap_or_default(),
            self.to.format(DATE_RANGE_FORMAT).unwrap_or_default()
        )
    }
}

/// `repos.minCreated`: GitHub was founded in February 2008.
fn min_created() -> OffsetDateTime {
    Date::from_calendar_date(2007, time::Month::June, 1)
        .expect("valid date")
        .with_time(Time::MIDNIGHT)
        .assume_utc()
}

/// `repos.repositoryQuery`.
#[derive(Debug, Clone)]
struct RepositoryQuery {
    query: String,
    created_from: OffsetDateTime,
    created_to: OffsetDateTime,
    cursor: String,
    first: i32,
    limit: i32,
    known: bool,
    count: i32,
}

impl RepositoryQuery {
    /// `repos.newRepositoryQuery`.
    fn new(query: &str, now: OffsetDateTime) -> Self {
        let mut query = query.to_owned();
        let date_range = strip_date_range(&mut query).unwrap_or(DateRange {
            from: min_created(),
            to: now,
        });
        let created_from = if date_range.from == OffsetDateTime::UNIX_EPOCH {
            min_created()
        } else {
            date_range.from
        };
        let created_to = if date_range.to == OffsetDateTime::UNIX_EPOCH {
            now
        } else {
            date_range.to
        };
        Self {
            query,
            created_from,
            created_to,
            cursor: String::new(),
            first: 0,
            limit: 0,
            known: false,
            count: 0,
        }
    }

    /// `repositoryQuery.String`.
    fn string(&self) -> String {
        format!(
            "{} created:{}",
            self.query,
            DateRange {
                from: self.created_from,
                to: self.created_to,
            }
            .string()
        )
    }
}

/// `repos.stripDateRange`.
fn strip_date_range(query: &mut String) -> Option<DateRange> {
    let regex = Regex::new(r"created:([^\s]+)").expect("static regex");
    let captures = regex.captures(query)?;
    let date_str = captures.get(1)?.as_str().to_owned();
    let full_match = captures.get(0)?.as_str().to_owned();

    let mut from_date_str = String::new();
    let mut to_date_str = String::new();
    let mut from_time_add = time::Duration::ZERO;
    let mut to_time_add = time::Duration::ZERO;
    let mut to_end_of_day = false;

    if let Some(rest) = date_str.strip_prefix(">=") {
        from_date_str = rest.to_owned();
    } else if let Some(rest) = date_str.strip_prefix('>') {
        from_date_str = rest.to_owned();
        from_time_add = time::Duration::seconds(1);
    } else if let Some(rest) = date_str.strip_prefix("<=") {
        to_date_str = rest.to_owned();
        to_end_of_day = true;
    } else if let Some(rest) = date_str.strip_prefix('<') {
        to_date_str = rest.to_owned();
        to_time_add = time::Duration::seconds(-1);
    } else {
        let mut range_parts = date_str.split("..");
        let from = range_parts.next()?;
        let to = range_parts.next()?;
        if range_parts.next().is_some() {
            return None;
        }
        from_date_str = from.to_owned();
        to_date_str = to.to_owned();
        if to != "*" {
            to_end_of_day = true;
        }
    }

    let mut range = DateRange {
        from: OffsetDateTime::UNIX_EPOCH,
        to: OffsetDateTime::UNIX_EPOCH,
    };
    if !from_date_str.is_empty() && from_date_str != "*" {
        range.from = parse_date(&from_date_str, false)? + from_time_add;
    }
    if !to_date_str.is_empty() && to_date_str != "*" {
        range.to = parse_date(&to_date_str, to_end_of_day)? + to_time_add;
    }
    *query = query.replace(&full_match, "");
    Some(range)
}

fn parse_date(value: &str, until_end_of_day: bool) -> Option<OffsetDateTime> {
    if value.contains('T') {
        if value.contains('+') || value.contains('Z') {
            return OffsetDateTime::parse(value, &Rfc3339).ok();
        }
        let format =
            time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
        return PrimitiveDateTime::parse(value, &format)
            .ok()
            .map(PrimitiveDateTime::assume_utc);
    }
    let format = time::macros::format_description!("[year]-[month]-[day]");
    let mut date = Date::parse(value, &format).ok()?.with_time(Time::MIDNIGHT);
    if until_end_of_day {
        date += time::Duration::days(1) - time::Duration::seconds(1);
    }
    Some(date.assume_utc())
}

#[async_trait]
impl Source for GitHubSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut unfiltered = Vec::new();
        self.collect_unfiltered(&mut unfiltered).await;

        let mut seen: HashSet<i64> = HashSet::new();
        let mut results = Vec::with_capacity(unfiltered.len());
        for result in unfiltered {
            match result.repo {
                Some(repo) => {
                    if !seen.contains(&repo.database_id) && !self.excludes(&repo) {
                        results.push(SourceResult::repo(self.make_repo(&repo)));
                        seen.insert(repo.database_id);
                    }
                }
                None => results.push(SourceResult {
                    repo: None,
                    err: result.err,
                }),
            }
        }
        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        self.v3
            .get_authenticated_user()
            .await
            .map(|_| ())
            .map_err(|err| format!("connection check failed: {err}"))
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytesize_parses_the_supported_units() {
        assert_eq!(parse_bytesize("10B").expect("B"), 10);
        assert_eq!(parse_bytesize("10kB").expect("kB"), 10_000);
        assert_eq!(parse_bytesize("10KiB").expect("KiB"), 10_240);
        assert_eq!(parse_bytesize("2MB").expect("MB"), 2_000_000);
        assert_eq!(parse_bytesize("2MiB").expect("MiB"), 2_097_152);
        assert!(parse_bytesize("10TB").is_err());
        assert!(parse_bytesize("MB").is_err());
    }

    #[test]
    fn org_matcher_matches_the_baseline_pattern() {
        assert_eq!(match_org("org:acme"), Some("acme".to_owned()));
        assert_eq!(match_org("org:acme/widgets"), None);
        assert_eq!(match_org("org:-acme"), None);
        assert_eq!(match_org("repo:org:acme"), None);
    }

    fn repo(name: &str, id: &str, database_id: i64) -> Repository {
        Repository {
            id: id.to_owned(),
            database_id,
            name_with_owner: name.to_owned(),
            ..Repository::default()
        }
    }

    #[test]
    fn exclusion_rules_are_anded_per_configuration_entry() {
        let excluder = GitHubExcluder::new(&[ExcludedGitHubRepo {
            name: "acme/secret".to_owned(),
            pattern: "secret".to_owned(),
            ..ExcludedGitHubRepo::default()
        }])
        .expect("rules");
        assert!(excluder.excludes(&repo("acme/secret", "id", 1)));
        assert!(!excluder.excludes(&repo("acme/secret-public", "id2", 2)));
        assert!(!excluder.excludes(&repo("other/secret", "id3", 3)));
    }

    #[test]
    fn generic_predicates_only_apply_to_repositories() {
        let excluder = GitHubExcluder::new(&[ExcludedGitHubRepo {
            archived: true,
            forks: true,
            ..ExcludedGitHubRepo::default()
        }])
        .expect("rules");
        // A string input never satisfies a generic rule.
        assert!(!excluder.should_exclude_str("acme/x"));
        let mut repository = repo("acme/x", "id", 1);
        repository.is_archived = true;
        repository.is_fork = true;
        assert!(excluder.should_exclude_repo(&repository));
        repository.is_fork = false;
        assert!(!excluder.should_exclude_repo(&repository));
    }

    #[test]
    fn size_and_star_constraints_compare_against_repo_fields() {
        let excluder = GitHubExcluder::new(&[ExcludedGitHubRepo {
            size: "> 1MiB".to_owned(),
            stars: "< 10".to_owned(),
            ..ExcludedGitHubRepo::default()
        }])
        .expect("rules");
        let mut repository = repo("acme/x", "id", 1);
        repository.disk_usage_kibibytes = 2048;
        repository.stargazer_count = 5;
        assert!(excluder.should_exclude_repo(&repository));
        repository.stargazer_count = 10;
        assert!(!excluder.should_exclude_repo(&repository));
        repository.disk_usage_kibibytes = 0;
        assert!(!excluder.should_exclude_repo(&repository));
    }

    #[test]
    fn created_ranges_are_stripped_and_defaulted() {
        let mut query = "org:acme created:2020-01-01..2020-12-31 fork:true".to_owned();
        let range = strip_date_range(&mut query).expect("range");
        assert_eq!(query, "org:acme  fork:true");
        assert_eq!(
            range.from,
            Date::from_calendar_date(2020, time::Month::January, 1)
                .expect("date")
                .with_time(Time::MIDNIGHT)
                .assume_utc()
        );
        assert_eq!(
            range.to,
            Date::from_calendar_date(2020, time::Month::December, 31)
                .expect("date")
                .with_time(Time::MIDNIGHT)
                .assume_utc()
                + time::Duration::days(1)
                - time::Duration::seconds(1)
        );

        let mut query = "org:acme".to_owned();
        assert!(strip_date_range(&mut query).is_none());
        let now = OffsetDateTime::now_utc();
        let built = RepositoryQuery::new("org:acme", now);
        assert_eq!(built.created_from, min_created());
        assert_eq!(built.created_to, now);
        assert!(built
            .string()
            .starts_with("org:acme created:2007-06-01T00:00:00"));
    }

    #[test]
    fn operator_comparisons_match() {
        assert!(Operator::Less.eval(1, 2));
        assert!(Operator::LessOrEqual.eval(2, 2));
        assert!(!Operator::Greater.eval(2, 2));
        assert!(Operator::GreaterOrEqual.eval(2, 2));
        assert!(Operator::parse("=").is_err());
    }
}
