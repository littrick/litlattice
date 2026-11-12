#![forbid(unsafe_code)]

//! `internal/repos/perforce.go`: the repo-updater Perforce source.
//!
//! The pure parts of the source (configuration decoding, `makeRepo`, clone-URL
//! composition, the `{depot}` repository name) are ported. Repository
//! enumeration and the connection check need the gitserver's
//! `IsPerforcePathCloneable`/`CheckPerforceCredentials` RPCs; the Rust
//! gitserver does not expose them and this slice does not add RPCs. A source
//! constructed through `new_source` therefore has no gitserver boundary and
//! reports an explicit not-ported error per depot (and from
//! `check_connection`). [`PerforceSource::new_with_gitserver`] accepts an
//! injected [`PerforceGitserver`] so the enumeration path is testable and can
//! be wired once the RPCs exist.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::perforce::{
    compose_perforce_clone_url, Depot, PerforceConnectionDetails, PerforceGitserver, TYPE_PERFORCE,
};
use crate::reposource::perforce_repo_name;
use crate::{ExternalRepoSpec, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `schema.PerforceAuthorization`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PerforceAuthorization {
    #[serde(rename = "identityProvider", default)]
    pub identity_provider: String,
}

/// `schema.PerforceConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PerforceConnection {
    #[serde(default)]
    pub authorization: Option<PerforceAuthorization>,
    #[serde(default)]
    pub depots: Vec<String>,
    #[serde(rename = "p4.passwd", default)]
    pub p4_passwd: String,
    #[serde(rename = "p4.port", default)]
    pub p4_port: String,
    #[serde(rename = "p4.user", default)]
    pub p4_user: String,
    #[serde(rename = "repositoryPathPattern", default)]
    pub repository_path_pattern: String,
}

/// `repos.PerforceSource`.
pub struct PerforceSource {
    service: SourceService,
    config: PerforceConnection,
    gitserver: Option<Arc<dyn PerforceGitserver>>,
}

impl PerforceSource {
    /// `repos.NewPerforceSource`, without the gitserver client (the Rust
    /// gitserver has no Perforce RPCs; see the module docs).
    pub fn new(service: SourceService) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let config: PerforceConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;
        Ok(Self {
            service,
            config,
            gitserver: None,
        })
    }

    /// Build a source with an injected gitserver boundary. Used by tests and
    /// by a future gitserver RPC wiring.
    pub fn new_with_gitserver(
        service: SourceService,
        gitserver: Arc<dyn PerforceGitserver>,
    ) -> Result<Self, String> {
        let mut source = Self::new(service)?;
        source.gitserver = Some(gitserver);
        Ok(source)
    }

    pub fn config(&self) -> &PerforceConnection {
        &self.config
    }

    fn connection_details(&self) -> PerforceConnectionDetails {
        PerforceConnectionDetails {
            p4_port: self.config.p4_port.clone(),
            p4_user: self.config.p4_user.clone(),
            p4_passwd: self.config.p4_passwd.clone(),
        }
    }

    /// `PerforceSource.makeRepo`.
    pub fn make_repo(&self, depot: &str) -> Repo {
        let mut depot = depot.to_owned();
        if !depot.ends_with('/') {
            depot.push('/');
        }
        let name = depot.trim_matches('/').to_owned();
        let urn = self.service.urn();
        let clone_url = compose_perforce_clone_url(&self.config.p4_port, &depot);

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn,
                clone_url,
            },
        );

        Repo {
            name: perforce_repo_name(&self.config.repository_path_pattern, &name),
            uri: perforce_repo_name("", &name),
            external_repo: ExternalRepoSpec {
                id: depot.clone(),
                service_type: TYPE_PERFORCE.to_owned(),
                service_id: self.config.p4_port.clone(),
            },
            private: true,
            sources,
            metadata: Some(
                serde_json::to_value(Depot { depot })
                    .expect("Perforce depot metadata serialises"),
            ),
            ..Repo::default()
        }
    }
}

/// Error text for the missing gitserver RPCs.
pub const NOT_PORTED_CLONEABLE: &str = "perforce gitserver RPC IsPerforcePathCloneable is not \
                                        ported to the Rust gitserver";
/// Error text for the missing credentials RPC.
pub const NOT_PORTED_CREDENTIALS: &str = "perforce gitserver RPC CheckPerforceCredentials is not \
                                          ported to the Rust gitserver";

#[async_trait]
impl Source for PerforceSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let mut results = Vec::new();
        let connection = self.connection_details();
        for depot in &self.config.depots {
            match &self.gitserver {
                Some(gitserver) => {
                    match gitserver
                        .is_perforce_path_cloneable(&connection, depot)
                        .await
                    {
                        Ok(()) => results.push(SourceResult::repo(self.make_repo(depot))),
                        Err(err) => results.push(SourceResult::error(format!(
                            "checking if perforce path is cloneable: {err}"
                        ))),
                    }
                }
                None => results.push(SourceResult::error(format!(
                    "checking if perforce path is cloneable: {NOT_PORTED_CLONEABLE}"
                ))),
            }
        }
        results
    }

    async fn check_connection(&self) -> Result<(), String> {
        let connection = self.connection_details();
        match &self.gitserver {
            Some(gitserver) => gitserver
                .check_perforce_credentials(&connection)
                .await
                .map_err(|err| format!("Unable to connect to the Perforce server: {err}")),
            None => Err(format!(
                "Unable to connect to the Perforce server: {NOT_PORTED_CREDENTIALS}"
            )),
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

    struct FakeGitserver {
        cloneable: Result<(), String>,
        checked: Mutex<usize>,
    }

    #[async_trait]
    impl PerforceGitserver for FakeGitserver {
        async fn is_perforce_path_cloneable(
            &self,
            _connection: &PerforceConnectionDetails,
            _depot: &str,
        ) -> Result<(), String> {
            self.cloneable.clone()
        }

        async fn check_perforce_credentials(
            &self,
            _connection: &PerforceConnectionDetails,
        ) -> Result<(), String> {
            *self.checked.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn service(config: &str) -> SourceService {
        SourceService {
            id: 9,
            kind: crate::KIND_PERFORCE.to_owned(),
            display_name: "perforce".to_owned(),
            config: config.to_owned(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    const CONFIG: &str = r#"{"p4.port":"ssl:111.222.333.444:1666","p4.user":"u","p4.passwd":"p","depots":["//Sourcegraph/","//Other"]}"#;

    #[test]
    fn make_repo_matches_the_baseline_shape() {
        let source = PerforceSource::new(service(CONFIG)).unwrap();
        let repo = source.make_repo("//Sourcegraph");
        assert_eq!(repo.name, "Sourcegraph");
        assert_eq!(repo.uri, "Sourcegraph");
        assert_eq!(repo.external_repo.id, "//Sourcegraph/");
        assert_eq!(repo.external_repo.service_type, "perforce");
        assert_eq!(repo.external_repo.service_id, "ssl:111.222.333.444:1666");
        assert!(repo.private);
        let info = repo.sources.get("extsvc:perforce:9").expect("source");
        assert_eq!(
            info.clone_url,
            "perforce://ssl:111.222.333.444:1666//Sourcegraph/"
        );
        assert_eq!(
            repo.metadata.unwrap(),
            serde_json::json!({"depot": "//Sourcegraph/"})
        );

        let named = PerforceSource::new(service(
            r#"{"p4.port":"host:1666","repositoryPathPattern":"perforce/{depot}","depots":[]}"#,
        ))
        .unwrap()
        .make_repo("//Depot/Sub");
        assert_eq!(named.name, "perforce/Depot/Sub");
        assert_eq!(named.uri, "Depot/Sub");
    }

    #[tokio::test]
    async fn without_gitserver_every_depot_is_not_ported() {
        let source = PerforceSource::new(service(CONFIG)).unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 2);
        for result in results {
            assert_eq!(
                result.err.unwrap(),
                format!(
                    "checking if perforce path is cloneable: {NOT_PORTED_CLONEABLE}"
                )
            );
        }
        assert_eq!(
            source.check_connection().await.unwrap_err(),
            format!("Unable to connect to the Perforce server: {NOT_PORTED_CREDENTIALS}")
        );
    }

    #[tokio::test]
    async fn injected_gitserver_enumerates_depots() {
        let gitserver = Arc::new(FakeGitserver {
            cloneable: Ok(()),
            checked: Mutex::new(0),
        });
        let source = PerforceSource::new_with_gitserver(service(CONFIG), gitserver.clone()).unwrap();
        let results = source.list_repos().await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.repo.is_some()));
        assert_eq!(results[0].repo.as_ref().unwrap().name, "Sourcegraph");
        assert!(source.check_connection().await.is_ok());
        assert_eq!(*gitserver.checked.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn cloneable_errors_are_wrapped() {
        let gitserver = Arc::new(FakeGitserver {
            cloneable: Err("boom".to_owned()),
            checked: Mutex::new(0),
        });
        let source = PerforceSource::new_with_gitserver(service(CONFIG), gitserver).unwrap();
        let results = source.list_repos().await;
        assert_eq!(
            results[0].err.as_deref(),
            Some("checking if perforce path is cloneable: boom")
        );
    }
}
