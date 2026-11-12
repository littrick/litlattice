#![forbid(unsafe_code)]

//! `internal/extsvc/npm/npm.go`: the npm registry client.
//!
//! Only the pieces the package source and the dependency-fetch paths use are
//! ported: `GetPackageInfo`, `GetDependencyInfo` and `FetchTarball`, with the
//! baseline's URL shapes, Bearer credentials, error strings and the
//! `not found` mapping for 404 responses.
//!
//! Rate limiting, the cached transport and observability are not ported; the
//! transport is injected as [`HttpDoer`](crate::HttpDoer).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::reposource_packages::NpmPackageName;
use crate::{HttpDoer, HttpRequest, HttpResponse};

/// `npm.PackageInfo`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct PackageInfo {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub versions: BTreeMap<String, DependencyInfo>,
}

/// `npm.DependencyInfo`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct DependencyInfo {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub dist: DependencyInfoDist,
}

/// `npm.DependencyInfoDist`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct DependencyInfoDist {
    #[serde(default, rename = "tarball")]
    pub tarball_url: String,
}

/// `npm.npmError`: 4xx/5xx responses from the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpmError {
    pub status_code: u16,
    pub message: String,
}

impl NpmError {
    pub fn not_found(&self) -> bool {
        self.status_code == 404
    }
}

impl std::fmt::Display for NpmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if (100..=599).contains(&self.status_code) {
            write!(f, "npm HTTP response {}: {}", self.status_code, self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}

/// `npm.HTTPClient`.
pub struct NpmClient {
    registry_url: String,
    credentials: String,
    doer: Arc<dyn HttpDoer>,
}

impl NpmClient {
    /// `npm.NewHTTPClient` without the client factory.
    pub fn new(
        registry_url: impl Into<String>,
        credentials: impl Into<String>,
        doer: Arc<dyn HttpDoer>,
    ) -> Self {
        Self {
            registry_url: registry_url.into(),
            credentials: credentials.into(),
            doer,
        }
    }

    pub fn registry_url(&self) -> &str {
        &self.registry_url
    }

    /// `HTTPClient.GetPackageInfo`.
    pub async fn get_package_info(&self, pkg: &NpmPackageName) -> Result<PackageInfo, String> {
        let url = format!("{}/{}", self.registry_url, pkg.package_syntax());
        let body = self.make_get_request(&url).await?;
        let pkg_info: PackageInfo =
            serde_json::from_slice(&body).map_err(|err| err.to_string())?;
        if pkg_info.versions.is_empty() {
            return Err("npm returned empty list of versions".to_owned());
        }
        Ok(pkg_info)
    }

    /// `HTTPClient.GetDependencyInfo`.
    pub async fn get_dependency_info(
        &self,
        pkg: &NpmPackageName,
        version: &str,
    ) -> Result<DependencyInfo, String> {
        let url = format!("{}/{}/{}", self.registry_url, pkg.package_syntax(), version);
        let body = self.make_get_request(&url).await?;
        serde_json::from_slice(&body)
            .map_err(|_| format!("unexpected JSON output from npm request: url={url}"))
    }

    /// `HTTPClient.FetchTarball`.
    pub async fn fetch_tarball(&self, tarball_url: &str) -> Result<Vec<u8>, String> {
        if tarball_url.is_empty() {
            return Err("empty TarballURL".to_owned());
        }
        self.make_get_request(tarball_url).await
    }

    /// `HTTPClient.makeGetRequest`.
    async fn make_get_request(&self, url: &str) -> Result<Vec<u8>, String> {
        let mut headers = Vec::new();
        if !self.credentials.is_empty() {
            headers.push((
                "Authorization".to_owned(),
                format!("Bearer {}", self.credentials),
            ));
        }
        let response = self
            .doer
            .do_request(HttpRequest {
                method: "GET".to_owned(),
                url: url.to_owned(),
                headers,
                body: Vec::new(),
            })
            .await?;

        if response.status >= 400 {
            return Err(NpmError {
                status_code: response.status,
                message: String::from_utf8_lossy(&response.body).into_owned(),
            }
            .to_string());
        }
        Ok(response.body)
    }
}

/// `httpcli.Doer`-level error for callers that need the status.
pub fn error_is_not_found(err: &str) -> bool {
    err.starts_with("npm HTTP response 404")
}

/// Builds a [`HttpResponse`] helper for tests.
pub fn response(status: u16, body: impl AsRef<[u8]>) -> HttpResponse {
    HttpResponse {
        status,
        headers: Vec::new(),
        body: body.as_ref().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct FakeDoer {
        responses: Mutex<Vec<HttpResponse>>,
        requests: Mutex<Vec<HttpRequest>>,
    }

    #[async_trait]
    impl HttpDoer for FakeDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(request);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(HttpResponse::default());
            }
            Ok(responses.remove(0))
        }
    }

    fn mk_client(responses: Vec<HttpResponse>) -> (NpmClient, Arc<FakeDoer>) {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
        });
        (NpmClient::new("https://registry.npmjs.org", "", doer.clone()), doer)
    }

    #[tokio::test]
    async fn package_info_parses_versions_and_reports_empty() {
        let (client, doer) = mk_client(vec![response(
            200,
            r#"{"description":"d","versions":{"1.0.0":{"dist":{"tarball":"https://x/a.tgz"}}}}"#,
        )]);
        let pkg = NpmPackageName::new("scope", "pkg").unwrap();
        let info = client.get_package_info(&pkg).await.unwrap();
        assert_eq!(info.description, "d");
        assert_eq!(
            info.versions["1.0.0"].dist.tarball_url,
            "https://x/a.tgz"
        );
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests[0].url, "https://registry.npmjs.org/@scope/pkg");

        let (empty_client, _) = mk_client(vec![response(200, r#"{"versions":{}}"#)]);
        assert_eq!(
            empty_client.get_package_info(&pkg).await.unwrap_err(),
            "npm returned empty list of versions"
        );
    }

    #[tokio::test]
    async fn credentials_and_errors_match_the_baseline() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![response(500, "boom")]),
            requests: Mutex::new(Vec::new()),
        });
        let client = NpmClient::new("https://r", "token", doer.clone());
        let pkg = NpmPackageName::new("", "pkg").unwrap();
        let err = client.get_dependency_info(&pkg, "1.0.0").await.unwrap_err();
        assert_eq!(err, "npm HTTP response 500: boom");
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests[0].url, "https://r/pkg/1.0.0");
        assert_eq!(
            requests[0].headers,
            vec![("Authorization".to_owned(), "Bearer token".to_owned())]
        );
    }

    #[tokio::test]
    async fn ill_formed_dependency_json_uses_the_baseline_error() {
        let (client, _) = mk_client(vec![response(200, "not json")]);
        let pkg = NpmPackageName::new("", "pkg").unwrap();
        assert_eq!(
            client.get_dependency_info(&pkg, "1").await.unwrap_err(),
            "unexpected JSON output from npm request: url=https://registry.npmjs.org/pkg/1"
        );
    }

    #[tokio::test]
    async fn fetch_tarball_requires_a_url() {
        let (client, _) = mk_client(Vec::new());
        assert_eq!(
            client.fetch_tarball("").await.unwrap_err(),
            "empty TarballURL"
        );
    }
}
