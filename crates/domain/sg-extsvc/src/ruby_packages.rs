#![forbid(unsafe_code)]

//! `internal/extsvc/rubygems/client.go`: fetch a `.gem` archive for a
//! versioned Ruby package. The transport is injected via
//! [`HttpDoer`](crate::HttpDoer); the self-imposed rate limiter and cached
//! transport are not ported.

use std::sync::Arc;

use url::Url;

use crate::{HttpDoer, HttpRequest};

/// `rubygems.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RubyGemsError {
    pub path: String,
    pub code: u16,
    pub message: String,
}

impl RubyGemsError {
    pub fn not_found(&self) -> bool {
        self.code == 404
    }
}

impl std::fmt::Display for RubyGemsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bad response with status code {} for {}: {}",
            self.code, self.path, self.message
        )
    }
}

/// `rubygems.Client`.
pub struct RubyGemsClient {
    registry_url: String,
    doer: Arc<dyn HttpDoer>,
}

impl RubyGemsClient {
    /// `rubygems.NewClient` without the client factory.
    pub fn new(registry_url: impl Into<String>, doer: Arc<dyn HttpDoer>) -> Self {
        Self {
            registry_url: registry_url.into(),
            doer,
        }
    }

    pub fn registry_url(&self) -> &str {
        &self.registry_url
    }

    /// `Client.GetPackageContents`.
    pub async fn get_package_contents(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Vec<u8>, String> {
        let repository = self.registry_url.strip_suffix('/').unwrap_or(&self.registry_url);
        let url = format!("{repository}/gems/{name}-{version}.gem");
        let response = self
            .doer
            .do_request(HttpRequest {
                method: "GET".to_owned(),
                url: url.clone(),
                headers: vec![(
                    "User-Agent".to_owned(),
                    "sourcegraph-rubygems-syncer (sourcegraph.com)".to_owned(),
                )],
                body: Vec::new(),
            })
            .await?;

        if response.status != 200 {
            let path = Url::parse(&url)
                .map(|parsed| parsed.path().to_owned())
                .map_err(|err| err.to_string())?;
            return Err(RubyGemsError {
                path,
                code: response.status,
                message: String::from_utf8_lossy(&response.body).into_owned(),
            }
            .to_string());
        }
        Ok(response.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpResponse;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct FakeDoer {
        response: Mutex<HttpResponse>,
        requests: Mutex<Vec<HttpRequest>>,
    }

    #[async_trait]
    impl HttpDoer for FakeDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(request);
            let response = self.response.lock().unwrap();
            Ok(HttpResponse {
                status: response.status,
                headers: Vec::new(),
                body: response.body.clone(),
            })
        }
    }

    fn client(status: u16, body: &str) -> (RubyGemsClient, Arc<FakeDoer>) {
        let doer = Arc::new(FakeDoer {
            response: Mutex::new(HttpResponse {
                status,
                headers: Vec::new(),
                body: body.as_bytes().to_vec(),
            }),
            requests: Mutex::new(Vec::new()),
        });
        (RubyGemsClient::new("https://rubygems.org/", doer.clone()), doer)
    }

    #[tokio::test]
    async fn builds_the_gem_url_and_sets_the_user_agent() {
        let (client, doer) = client(200, "gem");
        assert_eq!(
            client.get_package_contents("rails", "7.0.0").await.unwrap(),
            b"gem"
        );
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests[0].url, "https://rubygems.org/gems/rails-7.0.0.gem");
        assert_eq!(
            requests[0].headers,
            vec![(
                "User-Agent".to_owned(),
                "sourcegraph-rubygems-syncer (sourcegraph.com)".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn non_200_reports_the_error_text() {
        let (client, _) = client(404, "missing");
        assert_eq!(
            client.get_package_contents("rails", "1").await.unwrap_err(),
            "bad response with status code 404 for /gems/rails-1.gem: missing"
        );
    }
}
