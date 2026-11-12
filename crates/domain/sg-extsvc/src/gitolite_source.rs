#![forbid(unsafe_code)]

//! `internal/repos/gitolite.go`: the repo-updater Gitolite source.
//!
//! Discovery happens through an injectable [`GitoliteCommand`] seam instead of
//! the baseline's gitserver RPC (`gitserver.Client.ListGitoliteRepos`), which
//! shells out to `ssh` inside gitserver. The production seam runs the same
//! `ssh <host> info` command from repo-updater; tests inject fixed output.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::exclude::{ExcludeRule, Excluder};
use crate::gitolite::{decode_repos, external_repo_spec, service_id, GitoliteCommand};
use crate::reposource::gitolite_repo_name;
use crate::{Repo, Source, SourceInfo, SourceResult, SourceService};

/// `schema.ExcludedGitoliteRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedGitoliteRepo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pattern: String,
}

/// `schema.GitoliteConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitoliteConnection {
    #[serde(default)]
    pub exclude: Vec<ExcludedGitoliteRepo>,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub prefix: String,
}

/// `repos.GitoliteSource`.
pub struct GitoliteSource {
    service: SourceService,
    conn: GitoliteConnection,
    excluder: Excluder,
    command: Arc<dyn GitoliteCommand>,
}

impl GitoliteSource {
    /// `repos.NewGitoliteSource` with the gitserver client replaced by the
    /// injectable command seam.
    pub fn new(
        service: SourceService,
        command: Arc<dyn GitoliteCommand>,
    ) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let conn: GitoliteConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let rules: Vec<ExcludeRule> = conn
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
            conn,
            excluder,
            command,
        })
    }

    pub fn excluder(&self) -> &Excluder {
        &self.excluder
    }

    /// `GitoliteSource.excludes`: the excluder rules match the raw Gitolite
    /// name, while the illegal-character check matches the *mapped* repo name
    /// (which includes the configured prefix), exactly like the baseline.
    pub fn excludes(&self, repo: &crate::gitolite::Repo, mapped_name: &str) -> bool {
        self.excluder.should_exclude(&repo.name)
            || mapped_name
                .chars()
                .any(|c| "\\^$|()[]*?{},".contains(c))
    }

    /// `GitoliteSource.makeRepo`.
    pub fn make_repo(&self, repo: &crate::gitolite::Repo) -> Repo {
        let urn = self.service.urn();
        let name = gitolite_repo_name(&self.conn.prefix, &repo.name);

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url: repo.url.clone(),
            },
        );

        Repo {
            name: name.clone(),
            uri: name,
            description: String::new(),
            external_repo: external_repo_spec(repo, &service_id(&self.conn.host)),
            sources,
            metadata: Some(
                serde_json::to_value(repo).expect("Gitolite repo metadata serialises"),
            ),
            archived: false,
            fork: false,
            stars: 0,
            private: !self.service.unrestricted,
            ..Repo::default()
        }
    }
}

#[async_trait]
impl Source for GitoliteSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let info = match self.command.list_repos(&self.conn.host).await {
            Ok(info) => info,
            Err(err) => return vec![SourceResult::error(err)],
        };

        let mut results = Vec::new();
        for repo in decode_repos(&self.conn.host, &info) {
            let mapped = self.make_repo(&repo);
            if !self.excludes(&repo, &mapped.name) {
                results.push(SourceResult::repo(mapped));
            }
        }
        results
    }

    /// `GitoliteSource.CheckConnection` assumes availability.
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
    use crate::gitolite::Repo as GitoliteRepo;
    use crate::ExternalRepoSpec;

    struct FakeCommand {
        output: Result<String, String>,
    }

    #[async_trait]
    impl GitoliteCommand for FakeCommand {
        async fn list_repos(&self, _host: &str) -> Result<String, String> {
            self.output.clone()
        }
    }

    fn service(config: serde_json::Value) -> SourceService {
        SourceService {
            id: 7,
            kind: "GITOLITE".to_owned(),
            display_name: "gitolite".to_owned(),
            config: serde_json::to_string(&config).unwrap(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    #[tokio::test]
    async fn list_repos_maps_and_excludes() {
        let svc = service(serde_json::json!({
            "host": "git@gitolite.example.com",
            "prefix": "gitolite.example.com/",
            "exclude": [{"name": "secret"}]
        }));
        let command = Arc::new(FakeCommand {
            output: Ok("R W secret\nR W public\n".to_owned()),
        });
        let source = GitoliteSource::new(svc, command).unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        let repo = results[0].repo.as_ref().expect("repo");
        assert_eq!(repo.name, "gitolite.example.com/public");
        assert_eq!(repo.uri, "gitolite.example.com/public");
        assert_eq!(
            repo.external_repo,
            ExternalRepoSpec {
                id: "public".to_owned(),
                service_type: "gitolite".to_owned(),
                service_id: "git@gitolite.example.com".to_owned(),
            }
        );
        assert_eq!(
            repo.sources["extsvc:gitolite:7"].clone_url,
            "git@gitolite.example.com:public"
        );
        assert_eq!(
            repo.metadata,
            Some(serde_json::json!({"Name": "public", "URL": "git@gitolite.example.com:public"}))
        );
        assert!(repo.private);
    }

    #[test]
    fn make_repo_rewrites_at_sign_and_honours_prefix() {
        let svc = service(serde_json::json!({
            "host": "ssh://git@host:2222/",
            "prefix": "host/"
        }));
        let source = GitoliteSource::new(svc, Arc::new(FakeCommand { output: Ok(String::new()) }))
            .unwrap();
        let repo = source.make_repo(&GitoliteRepo {
            name: "team/repo@x".to_owned(),
            url: "ssh://git@host:2222/team/repo@x".to_owned(),
        });
        assert_eq!(repo.name, "host/team/repo-x");
        assert_eq!(repo.uri, "host/team/repo-x");
    }

    #[test]
    fn excludes_invalid_characters() {
        let svc = service(serde_json::json!({"host": "git@host"}));
        let source = GitoliteSource::new(svc, Arc::new(FakeCommand { output: Ok(String::new()) }))
            .unwrap();
        assert!(source.excludes(
            &GitoliteRepo {
                name: "a?b".to_owned(),
                ..GitoliteRepo::default()
            },
            "a?b"
        ));
        assert!(!source.excludes(
            &GitoliteRepo {
                name: "a/b".to_owned(),
                ..GitoliteRepo::default()
            },
            "a/b"
        ));
        // The prefix only participates in the illegal-character check; the
        // exclude rules keep matching the raw Gitolite name. Rebuild a source
        // with an exclusion rule to exercise both branches.
        let svc = service(serde_json::json!({
            "host": "git@host",
            "prefix": "host/",
            "exclude": [{"name": "secret"}]
        }));
        let source = GitoliteSource::new(svc, Arc::new(FakeCommand { output: Ok(String::new()) }))
            .unwrap();
        assert!(source.excludes(
            &GitoliteRepo {
                name: "secret".to_owned(),
                ..GitoliteRepo::default()
            },
            "host/secret"
        ));
        assert!(!source.excludes(
            &GitoliteRepo {
                name: "public".to_owned(),
                ..GitoliteRepo::default()
            },
            "host/public"
        ));
        assert!(source.excludes(
            &GitoliteRepo {
                name: "public".to_owned(),
                ..GitoliteRepo::default()
            },
            "host?/public"
        ));
    }

    #[tokio::test]
    async fn command_error_is_reported() {
        let svc = service(serde_json::json!({"host": "git@host"}));
        let source = GitoliteSource::new(
            svc,
            Arc::new(FakeCommand {
                output: Err("ssh: permission denied (publickey)".to_owned()),
            }),
        )
        .unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].err.as_deref(),
            Some("ssh: permission denied (publickey)")
        );
    }
}
