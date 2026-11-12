#![forbid(unsafe_code)]

//! `internal/repos/phabricator.go`: the repo-updater Phabricator source.
//!
//! The Conduit client is created and dialed lazily on the first `ListRepos`
//! (the baseline needs the call context for `conduit.getcapabilities`). Only
//! `git` repositories that are not `inactive` are yielded. The canonical name
//! comes from the first enabled non-builtin URI, or else the highest-priority
//! builtin URI; the clone URL follows the baseline's priority order, whose
//! later matches overwrite earlier ones.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::azuredevops::parse_urlx;
use crate::phabricator::{Client, Cursor, Repo as PhabricatorRepo, TYPE_PHABRICATOR};
use crate::{ExternalRepoSpec, HttpDoer, Repo, Source, SourceInfo, SourceResult, SourceService};

/// `schema.PhabricatorConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PhabricatorConnection {
    #[serde(default)]
    pub token: String,
    #[serde(default, rename = "url")]
    pub url: String,
}

/// `repos.PhabricatorSource`.
pub struct PhabricatorSource {
    service: SourceService,
    conn: PhabricatorConnection,
    doer: Arc<dyn HttpDoer>,
    client: Mutex<Option<Client>>,
}

impl PhabricatorSource {
    /// `repos.NewPhabricatorSource` with the HTTP transport injected; the
    /// client is dialed lazily like the baseline.
    pub fn new(service: SourceService, doer: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let conn: PhabricatorConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;
        Ok(Self {
            service,
            conn,
            doer,
            client: Mutex::new(None),
        })
    }

    /// `PhabricatorSource.client`: lazily construct and dial the Conduit
    /// client.
    async fn client(&self) -> Result<Client, String> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let client = Client::new(&self.conn.url, &self.conn.token, self.doer.clone())?;
        client.dial().await.map_err(|err| err.to_string())?;
        *guard = Some(client.clone());
        Ok(client)
    }

    /// `PhabricatorSource.makeRepo`.
    pub fn make_repo(&self, repo: &PhabricatorRepo) -> Result<Repo, String> {
        let mut external = Vec::new();
        let mut builtin: std::collections::BTreeMap<String, &crate::phabricator::Uri> =
            std::collections::BTreeMap::new();

        for uri in &repo.uris {
            if uri.disabled || uri.normalized.is_empty() {
                continue;
            }
            if !uri.builtin_identifier.is_empty() {
                builtin.insert(
                    format!("{}+{}", uri.builtin_protocol, uri.builtin_identifier),
                    uri,
                );
            } else {
                external.push(uri);
            }
        }

        let mut name = external.first().map(|uri| uri.normalized.clone()).unwrap_or_default();

        let mut clone_url = String::new();
        for (protocol, identifier) in [
            ("https", "shortname"),
            ("https", "callsign"),
            ("https", "id"),
            ("ssh", "shortname"),
            ("ssh", "callsign"),
            ("ssh", "id"),
        ] {
            if let Some(uri) = builtin.get(&format!("{protocol}+{identifier}")) {
                clone_url = uri.effective.clone();
                if name.is_empty() {
                    name = uri.normalized.clone();
                }
            }
        }

        if name.is_empty() {
            return Err(format!(
                "no canonical name available for repo with id={}",
                repo.phid
            ));
        }

        // `urlx.NormalizeString(s.conn.Url)`.
        let service_id = parse_urlx(&self.conn.url)?.string;
        let urn = self.service.urn();

        let mut sources = std::collections::BTreeMap::new();
        sources.insert(
            urn.clone(),
            SourceInfo {
                id: urn.clone(),
                clone_url,
            },
        );

        Ok(Repo {
            name: name.clone(),
            uri: name,
            description: String::new(),
            external_repo: ExternalRepoSpec {
                id: repo.phid.clone(),
                service_type: TYPE_PHABRICATOR.to_owned(),
                service_id,
            },
            sources,
            metadata: Some(
                serde_json::to_value(repo).expect("Phabricator repo metadata serialises"),
            ),
            archived: false,
            fork: false,
            stars: 0,
            private: !self.service.unrestricted,
            ..Repo::default()
        })
    }
}

#[async_trait]
impl Source for PhabricatorSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        let client = match self.client().await {
            Ok(client) => client,
            Err(err) => return vec![SourceResult::error(err)],
        };

        let mut cursor = Cursor {
            limit: 100,
            order: "oldest".to_owned(),
            ..Cursor::default()
        };
        let mut results = Vec::new();
        loop {
            let (page, next) = match client.list_repos(&cursor).await {
                Ok(page) => page,
                Err(err) => {
                    results.push(SourceResult::error(err.to_string()));
                    return results;
                }
            };
            cursor = next;

            for repo in &page {
                if repo.vcs != "git" || repo.status == "inactive" {
                    continue;
                }
                match self.make_repo(repo) {
                    Ok(mapped) => results.push(SourceResult::repo(mapped)),
                    Err(err) => {
                        results.push(SourceResult::error(err));
                        return results;
                    }
                }
            }

            if cursor.after.is_empty() {
                break;
            }
        }
        results
    }

    /// `PhabricatorSource.CheckConnection` assumes availability.
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
    use crate::phabricator::Uri;
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
                .position(|(path, _)| request.url.ends_with(path))
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

    fn source(responses: Vec<(&'static str, HttpResponse)>) -> PhabricatorSource {
        let service = SourceService {
            id: 11,
            kind: "PHABRICATOR".to_owned(),
            display_name: "phabricator".to_owned(),
            config: serde_json::json!({
                "url": "https://Phabricator.Example.com/",
                "token": "t"
            })
            .to_string(),
            encryption_key_id: String::new(),
            unrestricted: false,
        };
        PhabricatorSource::new(
            service,
            Arc::new(FixedDoer {
                responses: std::sync::Mutex::new(responses),
            }),
        )
        .unwrap()
    }

    fn uri(protocol: &str, identifier: &str, normalized: &str, effective: &str) -> Uri {
        Uri {
            phid: format!("PHID-URI-{normalized}"),
            display: normalized.to_owned(),
            effective: effective.to_owned(),
            normalized: normalized.to_owned(),
            builtin_protocol: protocol.to_owned(),
            builtin_identifier: identifier.to_owned(),
            ..Uri::default()
        }
    }

    #[test]
    fn make_repo_prefers_first_external_uri_and_last_builtin() {
        let source = source(Vec::new());
        let repo = PhabricatorRepo {
            phid: "PHID-REPO-1".to_owned(),
            name: "repo".to_owned(),
            vcs: "git".to_owned(),
            status: "active".to_owned(),
            uris: vec![
                uri("https", "shortname", "https://phab/r", "https://phab/r.git"),
                uri("ssh", "shortname", "ssh://phab/r", "ssh://phab/r.git"),
                Uri {
                    normalized: "https://mirror.example.com/r".to_owned(),
                    effective: "https://mirror.example.com/r".to_owned(),
                    ..Uri::default()
                },
            ],
            ..PhabricatorRepo::default()
        };
        let mapped = source.make_repo(&repo).unwrap();
        assert_eq!(mapped.name, "https://mirror.example.com/r");
        assert_eq!(
            mapped.sources["extsvc:phabricator:11"].clone_url,
            "ssh://phab/r.git"
        );
        assert_eq!(mapped.external_repo.service_id, "https://phabricator.example.com/");
        assert!(mapped.private);
    }

    #[test]
    fn make_repo_uses_builtin_normalized_name_when_no_external() {
        let source = source(Vec::new());
        let repo = PhabricatorRepo {
            phid: "PHID-REPO-2".to_owned(),
            uris: vec![uri("https", "shortname", "https://phab/r", "https://phab/r.git")],
            ..PhabricatorRepo::default()
        };
        let mapped = source.make_repo(&repo).unwrap();
        assert_eq!(mapped.name, "https://phab/r");
        assert_eq!(
            mapped.sources["extsvc:phabricator:11"].clone_url,
            "https://phab/r.git"
        );
    }

    #[test]
    fn make_repo_requires_a_name() {
        let source = source(Vec::new());
        let repo = PhabricatorRepo {
            phid: "PHID-REPO-3".to_owned(),
            ..PhabricatorRepo::default()
        };
        assert_eq!(
            source.make_repo(&repo).unwrap_err(),
            "no canonical name available for repo with id=PHID-REPO-3"
        );
    }

    #[tokio::test]
    async fn list_repos_dials_filters_and_pages() {
        let source = source(vec![
            (
                "/api/conduit.getcapabilities",
                json_response(serde_json::json!({
                    "result": {
                        "authentication": ["token"],
                        "input": ["urlencoded"],
                        "output": ["json"]
                    }
                })),
            ),
            (
                "/api/diffusion.repository.search",
                json_response(serde_json::json!({
                    "result": {
                        "data": [
                            {
                                "id": 1,
                                "phid": "PHID-REPO-1",
                                "fields": {
                                    "name": "a",
                                    "vcs": "git",
                                    "status": "active",
                                    "dateCreated": "100",
                                    "dateModified": "100"
                                },
                                "attachments": {"uris": {"uris": [
                                    {"fields": {
                                        "uri": {"display": "u", "effective": "u", "normalized": "https://phab/a"},
                                        "builtin": {"protocol": "https", "identifier": "shortname"}
                                    }}
                                ]}}
                            },
                            {"id": 2, "fields": {"name": "b", "vcs": "svn", "status": "active"}},
                            {"id": 3, "fields": {"name": "c", "vcs": "git", "status": "inactive"}}
                        ],
                        "cursor": {"after": ""}
                    }
                })),
            ),
        ]);
        let results = source.list_repos().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].repo.as_ref().unwrap().name, "https://phab/a");
    }

    #[tokio::test]
    async fn list_repos_ignores_unsupported_capabilities() {
        // Errata: gonduit v0.13.0 discards the `assertSupportedCapabilities`
        // error, so an unsupported token capability does not fail dialing; the
        // first real call proceeds (and would surface any failure).
        let source = source(vec![
            (
                "/api/conduit.getcapabilities",
                json_response(serde_json::json!({
                    "result": {"authentication": [], "input": ["urlencoded"], "output": ["json"]}
                })),
            ),
            (
                "/api/diffusion.repository.search",
                json_response(serde_json::json!({
                    "result": {"data": [], "cursor": {"after": ""}}
                })),
            ),
        ]);
        let results = source.list_repos().await;
        assert!(results.is_empty());
    }
}
