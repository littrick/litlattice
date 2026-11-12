#![forbid(unsafe_code)]

//! `internal/extsvc/crates/client.go`: GET a crate archive from crates.io.
//! The transport is injected via [`HttpDoer`](crate::HttpDoer); the
//! self-imposed rate limiter and cached transport are not ported.

use std::sync::Arc;

use url::Url;

use crate::{HttpDoer, HttpRequest};

/// `crates.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CratesError {
    pub path: String,
    pub code: u16,
    pub message: String,
}

impl CratesError {
    pub fn not_found(&self) -> bool {
        self.code == 404
    }
}

impl std::fmt::Display for CratesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bad response with status code {} for {}: {}",
            self.code, self.path, self.message
        )
    }
}

/// `crates.Client`.
pub struct CratesClient {
    doer: Arc<dyn HttpDoer>,
}

impl CratesClient {
    /// `crates.NewClient` without the client factory.
    pub fn new(doer: Arc<dyn HttpDoer>) -> Self {
        Self { doer }
    }

    /// `Client.Get`.
    pub async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        let response = self
            .doer
            .do_request(HttpRequest {
                method: "GET".to_owned(),
                url: url.to_owned(),
                headers: vec![(
                    "User-Agent".to_owned(),
                    "sourcegraph-crates-syncer (sourcegraph.com)".to_owned(),
                )],
                body: Vec::new(),
            })
            .await?;

        if response.status != 200 {
            let path = Url::parse(url)
                .map(|parsed| parsed.path().to_owned())
                .map_err(|err| err.to_string())?;
            return Err(CratesError {
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

    fn client(status: u16, body: &str) -> (CratesClient, Arc<FakeDoer>) {
        let doer = Arc::new(FakeDoer {
            response: Mutex::new(HttpResponse {
                status,
                headers: Vec::new(),
                body: body.as_bytes().to_vec(),
            }),
            requests: Mutex::new(Vec::new()),
        });
        (CratesClient::new(doer.clone()), doer)
    }

    #[tokio::test]
    async fn get_sets_the_user_agent() {
        let (client, doer) = client(200, "crate");
        assert_eq!(
            client
                .get("https://crates.io/api/v1/crates/serde/1.0.0/download")
                .await
                .unwrap(),
            b"crate"
        );
        let requests = doer.requests.lock().unwrap();
        assert_eq!(
            requests[0].headers,
            vec![(
                "User-Agent".to_owned(),
                "sourcegraph-crates-syncer (sourcegraph.com)".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn non_200_reports_the_error_text() {
        let (client, _) = client(410, "gone");
        assert_eq!(
            client
                .get("https://crates.io/api/v1/crates/serde/1.0.0/download")
                .await
                .unwrap_err(),
            "bad response with status code 410 for /api/v1/crates/serde/1.0.0/download: gone"
        );
    }
}
