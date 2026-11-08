#![forbid(unsafe_code)]

//! A minimal outbound HTTP client for source connections.
//!
//! The baseline builds these clients with `httpcli.ExternalClientFactory`
//! (`internal/httpcli`): proxy environment variables, TLS verification and
//! redirect handling all live there. This port keeps the subset the `OTHER`
//! source needs — `GET <url>` returning the response body, with no status
//! check, exactly like `internal/repos/other.go`'s `srcExpose`.

use std::collections::BTreeMap;
use std::sync::Arc;

use http::{HeaderName, HeaderValue, Method};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

#[derive(Clone)]
pub struct HttpClient {
    client: Arc<Client<HttpConnector, Full<Bytes>>>,
}

/// The response subset the outbound webhook sender stores in its log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    /// Canonicalized header names, e.g. `Content-Type`; values in wire order.
    pub headers: BTreeMap<String, Vec<String>>,
    pub body: Vec<u8>,
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

    /// `httpCli.Do(req)` for a `POST` with the given headers, returning the
    /// status, headers and full body (the outbound webhook sender's shape).
    pub async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResponse, String> {
        self.request(Method::POST, url, headers, body).await
    }

    /// `httpCli.Do(req)` for a `GET` with the given headers, returning the
    /// status, headers and full body (the Cody Gateway usage worker's shape).
    pub async fn get_with_headers(
        &self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.request(Method::GET, url, headers, Vec::new()).await
    }

    async fn request(
        &self,
        method: Method,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResponse, String> {
        let mut builder = http::Request::builder().method(method).uri(url);
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|err| format!("invalid header name {name:?}: {err}"))?;
            let value = HeaderValue::from_str(value)
                .map_err(|err| format!("invalid header value for {name}: {err}"))?;
            builder = builder.header(name, value);
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|err| format!("building request for {url}: {err}"))?;

        let response = self
            .client
            .request(request)
            .await
            .map_err(|err| format!("sending request to {url}: {err}"))?;
        let status = response.status().as_u16();

        let mut collected: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, value) in response.headers() {
            let name = canonical_header_name(name.as_str());
            let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
            collected.entry(name).or_default().push(value);
        }

        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|err| format!("failed to read response from {url}: {err}"))?;
        Ok(HttpResponse {
            status,
            headers: collected,
            body: body.to_bytes().to_vec(),
        })
    }
}

/// `net/http`'s `CanonicalHeaderKey`: upper-case the first letter and every
/// letter after a dash.
fn canonical_header_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = true;
    for byte in name.bytes() {
        if upper && byte.is_ascii_alphabetic() {
            out.push(char::from(byte.to_ascii_uppercase()));
        } else {
            out.push(char::from(byte));
        }
        upper = byte == b'-';
    }
    out
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
