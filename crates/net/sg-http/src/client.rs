#![forbid(unsafe_code)]

//! A minimal outbound HTTP client for source connections.
//!
//! The baseline builds these clients with `httpcli.ExternalClientFactory`
//! (`internal/httpcli`): proxy environment variables, TLS verification and
//! redirect handling all live there. This port keeps the subset the `OTHER`
//! source needs — `GET <url>` returning the response body, with no status
//! check, exactly like `internal/repos/other.go`'s `srcExpose`.

use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

#[derive(Clone)]
pub struct HttpClient {
    client: Arc<Client<HttpConnector, Full<Bytes>>>,
}

impl HttpClient {
    pub fn new() -> Self {
        let connector = HttpConnector::new();
        Self {
            client: Arc::new(Client::builder(TokioExecutor::new()).build(connector)),
        }
    }

    /// `httpCli.Do(req)` followed by `io.ReadAll(resp.Body)`.
    pub async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        let response = self
            .client
            .get(
                url.parse()
                    .map_err(|err| format!("invalid url {url:?}: {err}"))?,
            )
            .await
            .map_err(|err| format!("failed to fetch {url}: {err}"))?;
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|err| format!("failed to read response from {url}: {err}"))?;
        Ok(body.to_bytes().to_vec())
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_invalid_urls() {
        let client = HttpClient::new();
        let err = client.get("not a url").await.expect_err("invalid");
        assert!(err.starts_with("invalid url"), "{err}");
    }
}
