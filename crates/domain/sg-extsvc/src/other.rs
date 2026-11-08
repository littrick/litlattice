#![forbid(unsafe_code)]

//! `internal/repos/other.go`: the `OTHER` external service source.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::exclude::{ExcludeRule, Excluder};
use crate::reposource::OtherRepoSource;
use crate::{
    ExternalRepoSpec, HttpGetter, Repo, Source, SourceInfo, SourceResult, SourceService, TYPE_OTHER,
};

/// `schema.OtherExternalServiceConnection`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OtherConnection {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub repos: Vec<String>,
    #[serde(default, rename = "repositoryPathPattern")]
    pub repository_path_pattern: String,
    #[serde(default)]
    pub exclude: Vec<ExcludedRepo>,
    #[serde(default, rename = "makeReposPublicOnDotCom")]
    pub make_repos_public_on_dot_com: bool,
}

/// `schema.ExcludedOtherRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedRepo {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
}

/// `srcExposeItem`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SrcExposeItem {
    #[serde(default, alias = "uri", alias = "URI")]
    pub uri: String,
    #[serde(default, alias = "name", alias = "Name")]
    pub name: String,
    #[serde(
        default,
        rename = "clonePath",
        alias = "Clonepath",
        alias = "clonepath"
    )]
    pub clone_path: String,
    #[serde(
        default,
        rename = "AbsFilePath",
        alias = "absFilePath",
        alias = "absfilepath"
    )]
    pub abs_file_path: String,
}

#[derive(Debug, Default, Deserialize)]
struct SrcExposeResponse {
    #[serde(default, alias = "Items")]
    items: Vec<SrcExposeItem>,
}

/// `repos.OtherSource`.
pub struct OtherSource {
    service: SourceService,
    conn: OtherConnection,
    excluder: Excluder,
    http: Option<Arc<dyn HttpGetter>>,
    repo_source: OtherRepoSource,
}

impl OtherSource {
    /// `repos.NewOtherSource`. The caller passes the configuration with
    /// `svc.Config.Decrypt(ctx)` already applied (see the repo-updater
    /// source factory), so `service.config` is plaintext here.
    pub fn new(service: SourceService, http: Option<Arc<dyn HttpGetter>>) -> Result<Self, String> {
        let decoded = sg_config::strip_jsonc(&service.config);
        let conn: OtherConnection = serde_json::from_str(&decoded)
            .map_err(|err| format!("external service id={} config error: {err}", service.id))?;

        let rules: Vec<ExcludeRule> = conn
            .exclude
            .iter()
            .map(|rule| ExcludeRule {
                name: rule.name.clone(),
                pattern: rule.pattern.clone(),
            })
            .collect();
        let excluder = Excluder::new(&rules)?;

        let repo_source = OtherRepoSource {
            url: conn.url.clone(),
            repository_path_pattern: conn.repository_path_pattern.clone(),
        };

        Ok(Self {
            service,
            conn,
            excluder,
            http,
            repo_source,
        })
    }

    /// `OtherSource.srcExposeRequest`: a single `src-expose`/`src-serve`
    /// entry selects the src-serve listing API.
    pub fn src_expose_request(&self) -> Option<String> {
        if self.conn.repos.len() == 1
            && (self.conn.repos[0] == "src-expose" || self.conn.repos[0] == "src-serve")
        {
            Some(format!("{}/v1/list-repos", self.conn.url))
        } else {
            None
        }
    }

    /// `OtherSource.srcExpose`. `Ok(None)` means the configuration is not a
    /// src-expose one; `Err` carries the listing failure.
    pub async fn src_expose(&self) -> Result<Option<Vec<Repo>>, String> {
        let Some(request_url) = self.src_expose_request() else {
            return Ok(None);
        };
        let Some(client) = self.http.as_ref() else {
            return Err("src-expose HTTP client is not configured".to_owned());
        };
        let body = client.get(&request_url).await?;
        let text = String::from_utf8_lossy(&body);
        let response: SrcExposeResponse = serde_json::from_str(&text)
            .map_err(|_| format!("failed to decode response from src-expose: {text}"))?;

        let mut clone_prefix = self.conn.url.clone();
        if !clone_prefix.ends_with('/') {
            clone_prefix.push('/');
        }
        let urn = self.service.urn();
        let mut repos = Vec::with_capacity(response.items.len());
        for item in response.items {
            if item.uri.is_empty() {
                return Err(format!(
                    "repo without URI returned from src-expose: {item:?}"
                ));
            }
            let mut item = item;
            // `ClonePath` is always set by current src-cli versions.
            if item.clone_path.is_empty() && !item.uri.ends_with("/.git") {
                item.clone_path = format!("{}/.git", item.uri);
            }
            let clone_url = format!("{clone_prefix}{}", item.clone_path.trim_start_matches('/'));
            let name = if item.name.is_empty() {
                item.uri.clone()
            } else {
                item.name.clone()
            };
            let repo = Repo {
                name: name.strip_suffix(".git").unwrap_or(&name).to_owned(),
                uri: item.uri.clone(),
                external_repo: ExternalRepoSpec {
                    id: item.uri.clone(),
                    service_type: TYPE_OTHER.to_owned(),
                    service_id: self.conn.url.clone(),
                },
                sources: [(
                    urn.clone(),
                    SourceInfo {
                        id: urn.clone(),
                        clone_url: clone_url.clone(),
                    },
                )]
                .into_iter()
                .collect(),
                metadata: Some(serde_json::json!({
                    "RelativePath": clone_url.strip_prefix(&self.conn.url).unwrap_or(&clone_url),
                    "AbsFilePath": item.abs_file_path,
                })),
                private: !self.service.unrestricted,
                ..Repo::default()
            };
            if self.excluder.should_exclude(&repo.name) {
                continue;
            }
            repos.push(repo);
        }
        Ok(Some(repos))
    }

    /// `OtherSource.cloneURLs`.
    fn clone_urls(&self) -> Result<Vec<String>, String> {
        if self.conn.repos.is_empty() {
            return Ok(Vec::new());
        }
        let base = if self.conn.url.is_empty() {
            None
        } else {
            Some(self.conn.url.as_str())
        };
        self.conn
            .repos
            .iter()
            .map(|repo| resolve_clone_url(base, repo))
            .collect()
    }

    /// `OtherSource.otherRepoFromCloneURL`.
    pub fn other_repo_from_clone_url(&self, urn: &str, clone_url: &str) -> Result<Repo, String> {
        let repo_name = self.repo_source.clone_url_to_repo_name(clone_url)?;
        let repo_uri = self.repo_source.clone_url_to_repo_uri(clone_url)?;
        let service_id = url_without_path_and_query(clone_url);

        Ok(Repo {
            name: repo_name.clone(),
            uri: repo_uri,
            external_repo: ExternalRepoSpec {
                id: repo_name,
                service_type: TYPE_OTHER.to_owned(),
                service_id: service_id.clone(),
            },
            sources: [(
                urn.to_owned(),
                SourceInfo {
                    id: urn.to_owned(),
                    clone_url: clone_url.to_owned(),
                },
            )]
            .into_iter()
            .collect(),
            metadata: Some(serde_json::json!({
                "RelativePath": clone_url.strip_prefix(&service_id).unwrap_or(clone_url),
            })),
            private: !self.service.unrestricted,
            ..Repo::default()
        })
    }
}

#[async_trait]
impl Source for OtherSource {
    async fn list_repos(&self) -> Vec<SourceResult> {
        match self.src_expose().await {
            Err(err) => vec![SourceResult::error(err)],
            Ok(Some(repos)) => repos.into_iter().map(SourceResult::repo).collect(),
            Ok(None) => {
                let urn = self.service.urn();
                let urls = match self.clone_urls() {
                    Ok(urls) => urls,
                    Err(err) => return vec![SourceResult::error(err)],
                };
                let mut results = Vec::with_capacity(urls.len());
                for url in urls {
                    match self.other_repo_from_clone_url(&urn, &url) {
                        Ok(repo) => {
                            if !self.excluder.should_exclude(&repo.name) {
                                results.push(SourceResult::repo(repo));
                            }
                        }
                        Err(err) => {
                            // Like the baseline, results already sent stay.
                            results.push(SourceResult::error(err));
                            break;
                        }
                    }
                }
                results
            }
        }
    }

    fn external_services(&self) -> Vec<SourceService> {
        vec![self.service.clone()]
    }
}

fn has_scheme(value: &str) -> bool {
    value.find("://").is_some_and(|index| {
        let scheme = &value[..index];
        !scheme.is_empty()
            && scheme
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    })
}

/// `url.URL.String()` after clearing `Path` and `RawQuery`.
fn url_without_path_and_query(url: &str) -> String {
    let Some(index) = url.find("://") else {
        return String::new();
    };
    let scheme = &url[..index];
    let rest = &url[index + 3..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let fragment = rest.find('#').map(|i| &rest[i..]).unwrap_or("");
    format!("{scheme}://{authority}{fragment}")
}

/// `otherRepoCloneURL`: `base.Parse(repo)` or `url.Parse(repo)`, preserving
/// the string form for the common (non-escaping) cases.
fn resolve_clone_url(base: Option<&str>, repo: &str) -> Result<String, String> {
    if has_scheme(repo) {
        return Ok(repo.to_owned());
    }
    match base {
        None => {
            // Go's `url.Parse` accepts relative paths but rejects a relative
            // first segment containing a colon (SCP-style URLs).
            if repo
                .split('/')
                .next()
                .is_some_and(|segment| segment.contains(':'))
            {
                return Err(format!(
                    "parse {repo:?}: first path segment in URL cannot contain colon"
                ));
            }
            Ok(repo.to_owned())
        }
        Some(base) => {
            if !has_scheme(base) {
                return Err(format!("parse {base:?}: missing protocol scheme"));
            }
            let index = base.find("://").expect("checked scheme");
            let scheme = &base[..index];
            let rest = &base[index + 3..];
            let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
            let authority = &rest[..authority_end];
            let path_start = authority_end;
            let path_end = rest[path_start..]
                .find(['?', '#'])
                .map(|offset| path_start + offset)
                .unwrap_or(rest.len());
            let base_path = &rest[path_start..path_end];

            let path = if repo.starts_with('/') {
                repo.to_owned()
            } else if base_path.is_empty() || base_path == "/" {
                format!("/{repo}")
            } else if let Some(dir) = base_path.rfind('/') {
                format!("{}{repo}", &base_path[..=dir])
            } else {
                format!("/{repo}")
            };
            Ok(format!("{scheme}://{authority}{path}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(config: &str) -> SourceService {
        SourceService {
            id: 1,
            kind: "OTHER".to_owned(),
            display_name: "other".to_owned(),
            config: config.to_owned(),
            encryption_key_id: String::new(),
            unrestricted: false,
        }
    }

    struct StaticHttp(Vec<u8>);

    #[async_trait]
    impl HttpGetter for StaticHttp {
        async fn get(&self, _url: &str) -> Result<Vec<u8>, String> {
            Ok(self.0.clone())
        }
    }

    fn repo_names(results: &[SourceResult]) -> Vec<String> {
        results
            .iter()
            .map(|result| {
                assert!(result.err.is_none(), "{:?}", result.err);
                result.repo.as_ref().expect("repo").name.clone()
            })
            .collect()
    }

    #[tokio::test]
    async fn lists_static_clone_urls() {
        let source = OtherSource::new(
            service(r#"{"url": "http://test", "repos": ["a", "b/c", "d"]}"#),
            None,
        )
        .expect("source");
        assert_eq!(
            repo_names(&source.list_repos().await),
            ["test/a", "test/b/c", "test/d"]
        );
    }

    #[tokio::test]
    async fn static_repos_apply_the_path_pattern_and_exclusions() {
        let source = OtherSource::new(
            service(
                r#"{
                    "url": "http://test",
                    "repos": ["keep1", "not-exact/keep2", "exclude-dir/a", "exclude-dir/b", "exclude/exact", "keep3"],
                    "exclude": [{"name": "not-exact"}, {"name": "exclude/exact"}, {"pattern": "exclude-dir"}],
                    "repositoryPathPattern": "{repo}"
                }"#,
            ),
            None,
        )
        .expect("source");
        assert_eq!(
            repo_names(&source.list_repos().await),
            ["keep1", "not-exact/keep2", "keep3"]
        );
    }

    #[tokio::test]
    async fn lists_src_expose_items() {
        let body = br#"{"Items":[
            {"uri": "/repos/foo", "clonePath":"/repos/foo/.git"},
            {"uri":"/repos/bar/baz", "clonePath":"/repos/bar/baz/.git"}
        ]}"#;
        let source = OtherSource::new(
            service(r#"{"url": "http://test", "repos": ["src-expose"]}"#),
            Some(Arc::new(StaticHttp(body.to_vec()))),
        )
        .expect("source");
        let repos = source.src_expose().await.expect("no error").expect("valid");
        assert_eq!(repos.len(), 2);
        assert_eq!(repos[0].name, "/repos/foo");
        assert_eq!(repos[0].uri, "/repos/foo");
        assert_eq!(
            repos[0].sources["extsvc:other:1"].clone_url,
            "http://test/repos/foo/.git"
        );
        assert_eq!(
            repos[0].metadata.as_ref().unwrap()["RelativePath"],
            "/repos/foo/.git"
        );
        assert!(repos[0].private);
        assert_eq!(repos[1].name, "/repos/bar/baz");
    }

    #[tokio::test]
    async fn src_expose_decodes_lowercase_items_like_go() {
        let source = OtherSource::new(
            service(r#"{"url": "http://test", "repos": ["src-serve"]}"#),
            Some(Arc::new(StaticHttp(b"{\"items\":[]}".to_vec()))),
        )
        .expect("source");
        let repos = source.src_expose().await.expect("no error").expect("valid");
        assert!(repos.is_empty());
    }

    #[tokio::test]
    async fn src_expose_reports_decode_and_missing_uri_errors() {
        let source = OtherSource::new(
            service(r#"{"url": "http://test", "repos": ["src-expose"]}"#),
            Some(Arc::new(StaticHttp(b"boom".to_vec()))),
        )
        .expect("source");
        let err = source.src_expose().await.expect_err("decode");
        assert_eq!(err, "failed to decode response from src-expose: boom");

        let source = OtherSource::new(
            service(r#"{"url": "http://test", "repos": ["src-expose"]}"#),
            Some(Arc::new(StaticHttp(
                br#"{"Items":[{"name": "foo"}]}"#.to_vec(),
            ))),
        )
        .expect("source");
        let err = source.src_expose().await.expect_err("missing uri");
        assert!(err.starts_with("repo without URI returned from src-expose:"));
    }

    #[tokio::test]
    async fn name_pattern_uses_the_default_uri_pattern() {
        let source = OtherSource::new(
            service(
                r#"{"url": "https://github.com", "repos": ["gorilla/mux"], "repositoryPathPattern": "pre-{repo}"}"#,
            ),
            None,
        )
        .expect("source");
        let repos = source.list_repos().await;
        let repo = repos[0].repo.as_ref().expect("repo");
        assert_eq!(repo.name, "pre-gorilla/mux");
        assert_eq!(repo.uri, "github.com/gorilla/mux");
        assert_eq!(repo.external_repo.service_id, "https://github.com");
        assert_eq!(
            repo.sources["extsvc:other:1"].clone_url,
            "https://github.com/gorilla/mux"
        );
        assert_eq!(
            repo.metadata.as_ref().unwrap()["RelativePath"],
            "/gorilla/mux"
        );
    }

    #[test]
    fn relative_repos_are_joined_to_the_base() {
        assert_eq!(
            resolve_clone_url(Some("https://github.com"), "gorilla/mux").unwrap(),
            "https://github.com/gorilla/mux"
        );
        assert_eq!(
            resolve_clone_url(Some("https://github.com/"), "gorilla/mux").unwrap(),
            "https://github.com/gorilla/mux"
        );
        assert_eq!(
            resolve_clone_url(Some("https://github.com/base"), "gorilla/mux").unwrap(),
            "https://github.com/gorilla/mux"
        );
        assert_eq!(
            resolve_clone_url(Some("https://github.com/base/"), "gorilla/mux").unwrap(),
            "https://github.com/base/gorilla/mux"
        );
    }
}
