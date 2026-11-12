#![forbid(unsafe_code)]

//! `internal/extsvc/gomodproxy/client.go`: the Go module proxy client.
//!
//! Ports `GetVersion` (`@v/<escaped version>.info` or `@latest`) and
//! `GetZip` (`@v/<escaped version>.zip`), including the multi-URL fallback
//! where only 404/410 responses advance to the next proxy. The transport is
//! injected via [`HttpDoer`](crate::HttpDoer); rate limiting, the cached
//! transport and observability are not ported.

use std::sync::Arc;

use url::Url;

use crate::go_modules;
use crate::gitlab::go_path_join;
use crate::reposource_packages::go_quote;
use crate::{HttpDoer, HttpRequest};

/// `gomodproxy.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoModProxyError {
    pub path: String,
    pub code: u16,
    pub message: String,
}

impl GoModProxyError {
    pub fn not_found(&self) -> bool {
        self.code == 404 || self.code == 410
    }
}

impl std::fmt::Display for GoModProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bad go module proxy response with status code {} for {}: {}",
            self.code, self.path, self.message
        )
    }
}

enum GetError {
    NotFound(GoModProxyError),
    Fatal(String),
}

impl GetError {
    fn into_string(self) -> String {
        match self {
            Self::NotFound(err) => err.to_string(),
            Self::Fatal(err) => err,
        }
    }
}

/// `gomodproxy.Client`.
pub struct GoModProxyClient {
    urls: Vec<String>,
    doer: Arc<dyn HttpDoer>,
}

impl GoModProxyClient {
    /// `gomodproxy.NewClient` without the client factory.
    pub fn new(urls: Vec<String>, doer: Arc<dyn HttpDoer>) -> Self {
        Self { urls, doer }
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    /// `Client.GetVersion`.
    pub async fn get_version(
        &self,
        module: &str,
        version: &str,
    ) -> Result<go_modules::ModuleVersion, String> {
        let paths: Vec<String> = if version.is_empty() {
            vec!["@latest".to_owned()]
        } else {
            let escaped_version = go_modules::escape_version(version)
                .map_err(|err| format!("failed to escape version: {err}"))?;
            vec!["@v".to_owned(), format!("{escaped_version}.info")]
        };
        let body = self.get(&self.doer, module, &paths).await?;

        #[derive(serde::Deserialize)]
        struct VersionBody {
            #[serde(default, rename = "Version", alias = "version")]
            version: String,
        }
        let decoded: VersionBody = serde_json::from_slice(&body).map_err(|err| err.to_string())?;
        Ok(go_modules::ModuleVersion {
            path: module.to_owned(),
            version: decoded.version,
        })
    }

    /// `Client.GetZip`.
    pub async fn get_zip(&self, module: &str, version: &str) -> Result<Vec<u8>, String> {
        let escaped_version = go_modules::escape_version(version)
            .map_err(|err| format!("failed to escape version: {err}"))?;
        self.get(
            &self.doer,
            module,
            &["@v".to_owned(), format!("{escaped_version}.zip")],
        )
        .await
    }

    /// `Client.get`.
    async fn get(
        &self,
        doer: &Arc<dyn HttpDoer>,
        module: &str,
        paths: &[String],
    ) -> Result<Vec<u8>, String> {
        let escaped_module =
            go_modules::escape_path(module).map_err(|err| format!("failed to escape module path: {err}"))?;
        let request_path = go_path_join(&escaped_module, &paths.join("/"));

        let mut last: Option<GetError> = None;
        for base_url in &self.urls {
            let url = match build_url(base_url, &request_path) {
                Ok(url) => url,
                Err(_) => {
                    return Err(format!(
                        "invalid go modules proxy URL {}",
                        go_quote(base_url)
                    ))
                }
            };

            match do_request(doer, &url, &request_path).await {
                Ok(body) => return Ok(body),
                Err(err) => {
                    let not_found = matches!(&err, GetError::NotFound(inner) if inner.not_found());
                    last = Some(err);
                    if !not_found {
                        break;
                    }
                }
            }
        }

        match last {
            Some(err) => Err(err.into_string()),
            None => Ok(Vec::new()),
        }
    }
}

/// Replaces the whole path of `base_url` with `request_path`, like the
/// baseline's `reqURL.Path = path.Join(...)`.
fn build_url(base_url: &str, request_path: &str) -> Result<String, String> {
    let mut url = Url::parse(base_url).map_err(|err| err.to_string())?;
    let path = if request_path.starts_with('/') {
        request_path.to_owned()
    } else {
        format!("/{request_path}")
    };
    url.set_path(&path);
    Ok(url.to_string())
}

async fn do_request(
    doer: &Arc<dyn HttpDoer>,
    url: &str,
    path: &str,
) -> Result<Vec<u8>, GetError> {
    let response = doer
        .do_request(HttpRequest {
            method: "GET".to_owned(),
            url: url.to_owned(),
            headers: Vec::new(),
            body: Vec::new(),
        })
        .await
        .map_err(GetError::Fatal)?;

    if response.status != 200 {
        // The baseline reports `req.URL.Path`, which always starts with `/`.
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        let error = GoModProxyError {
            path,
            code: response.status,
            message: String::from_utf8_lossy(&response.body).into_owned(),
        };
        return Err(if error.not_found() {
            GetError::NotFound(error)
        } else {
            GetError::Fatal(error.to_string())
        });
    }
    Ok(response.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpResponse;
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

    fn response(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    fn client(responses: Vec<HttpResponse>) -> (GoModProxyClient, Arc<FakeDoer>) {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
        });
        (
            GoModProxyClient::new(vec!["https://proxy.golang.org".to_owned()], doer.clone()),
            doer,
        )
    }

    #[tokio::test]
    async fn get_version_uses_the_info_path() {
        let (client, doer) = client(vec![response(200, r#"{"Version":"v1.2.3"}"#)]);
        let version = client
            .get_version("github.com/gorilla/mux", "v1.2.3")
            .await
            .unwrap();
        assert_eq!(version.version, "v1.2.3");
        let requests = doer.requests.lock().unwrap();
        assert_eq!(
            requests[0].url,
            "https://proxy.golang.org/github.com/gorilla/mux/@v/v1.2.3.info"
        );
    }

    #[tokio::test]
    async fn get_version_without_version_uses_latest() {
        let (client, doer) = client(vec![response(200, r#"{"Version":"v2.0.0"}"#)]);
        client.get_version("example.com/mod", "").await.unwrap();
        let requests = doer.requests.lock().unwrap();
        assert_eq!(
            requests[0].url,
            "https://proxy.golang.org/example.com/mod/@latest"
        );
    }

    #[tokio::test]
    async fn not_found_falls_back_to_the_next_proxy() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![response(404, "no"), response(200, r#"{"Version":"v1"}"#)]),
            requests: Mutex::new(Vec::new()),
        });
        let client = GoModProxyClient::new(
            vec!["https://a.example".to_owned(), "https://b.example".to_owned()],
            doer.clone(),
        );
        let version = client.get_version("example.com/mod", "v1.0.0").await.unwrap();
        assert_eq!(version.version, "v1");
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].url, "https://b.example/example.com/mod/@v/v1.0.0.info");
    }

    #[tokio::test]
    async fn other_errors_do_not_fall_back_and_keep_the_error_text() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![response(500, "boom")]),
            requests: Mutex::new(Vec::new()),
        });
        let client = GoModProxyClient::new(
            vec!["https://a.example".to_owned(), "https://b.example".to_owned()],
            doer.clone(),
        );
        assert_eq!(
            client.get_version("example.com/mod", "v1.0.0").await.unwrap_err(),
            "bad go module proxy response with status code 500 for /example.com/mod/@v/v1.0.0.info: boom"
        );
        assert_eq!(doer.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_zip_escapes_the_version() {
        let (client, doer) = client(vec![response(200, "zip")]);
        assert_eq!(
            client.get_zip("github.com/Azure/mod", "v1.0.0").await.unwrap(),
            b"zip"
        );
        let requests = doer.requests.lock().unwrap();
        assert!(
            requests[0].url.contains("/github.com/!azure/mod/@v/v1.0.0.zip")
                || requests[0]
                    .url
                    .contains("/github.com/%21azure/mod/@v/v1.0.0.zip")
        );
    }
}
