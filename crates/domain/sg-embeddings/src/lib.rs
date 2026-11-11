#![forbid(unsafe_code)]

//! `sg-embeddings`: the embeddings provider clients used by the worker's
//! `repo-embedding-job`.
//!
//! Ports `internal/embeddings/embed/client/{client,sourcegraph,openai,
//! azureopenai,modeltransformations}`:
//!
//! * [`EmbeddingsClient`] — `client.EmbeddingsClient` (`GetQueryEmbedding`,
//!   `GetDocumentEmbeddings`, `GetDimensions`, `GetModelIdentifier`);
//! * [`SourcegraphClient`] — the Cody Gateway client
//!   (`sourcegraph/client.go`);
//! * [`OpenAiClient`] — `openai/client.go`;
//! * [`AzureOpenAiClient`] — `azureopenai/client.go`, implemented as the raw
//!   Azure REST request the `azopenai` SDK v0.5.0 emits;
//! * [`modeltransformations`] — E5 prefixes and the newline-stripping model.
//!
//! The HTTP layer is a [`HttpTransport`] seam so the differential harness can
//! record requests and script responses without a network. [`ReqwestTransport`]
//! is the production implementation.
//!
//! ## Deviations
//!
//! * Azure OpenAI without an access token used the Azure identity SDK
//!   (`DefaultAzureCredential`); that path is not ported and the factory
//!   returns an explicit error. With a token, requests carry `api-key`.
//! * The baseline returns `(nil, nil)` for an empty embeddings response; Rust
//!   returns an empty [`EmbeddingsResults`] (dimensions `0`), which the caller
//!   then rejects via its batch-length check instead of dereferencing nil.
//! * Logging/metrics/tracing are not ported.
//!
//! See `docs/overviews/m9_8it_embeddings_client_overview.md`.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

pub mod azureopenai;
pub mod modeltransformations;
pub mod openai;
pub mod sourcegraph;

pub use azureopenai::AzureOpenAiClient;
pub use openai::OpenAiClient;
pub use sourcegraph::SourcegraphClient;

/// `conftypes.EmbeddingsProviderName`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    Sourcegraph,
    OpenAI,
    AzureOpenAI,
}

impl Provider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sourcegraph => "sourcegraph",
            Self::OpenAI => "openai",
            Self::AzureOpenAI => "azure-openai",
        }
    }

    /// `switch config.Provider` in `embed.NewEmbeddingsClient`; `Err` carries
    /// the baseline `invalid provider %q` message.
    pub fn try_from_name(name: &str) -> Result<Self, String> {
        match name {
            "sourcegraph" => Ok(Self::Sourcegraph),
            "openai" => Ok(Self::OpenAI),
            "azure-openai" => Ok(Self::AzureOpenAI),
            other => Err(format!("invalid provider {other:?}")),
        }
    }
}

/// The provider fields of `conftypes.EmbeddingsConfig` the clients read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    pub provider: Provider,
    pub access_token: String,
    pub model: String,
    pub endpoint: String,
    pub dimensions: i64,
}

/// `client.EmbeddingsResults`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EmbeddingsResults {
    pub embeddings: Vec<f32>,
    /// Indices of input texts that failed to get embeddings.
    pub failed: Vec<i64>,
    pub dimensions: usize,
}

impl EmbeddingsResults {
    pub fn row(&self, n: usize) -> &[f32] {
        &self.embeddings[n * self.dimensions..(n + 1) * self.dimensions]
    }

    /// An empty response (`len(response.Embeddings) == 0` in the baseline).
    pub fn is_empty_response(&self) -> bool {
        self.embeddings.is_empty() && self.failed.is_empty() && self.dimensions == 0
    }
}

/// How a `retry-after` header was parsed (`client.RateLimitExceededError`'s
/// `retryAfter time.Time`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAfter {
    /// The header was an integer number of seconds: `time.Now().Add(seconds)`.
    Seconds(i64),
    /// The header was an RFC1123 date.
    Date(OffsetDateTime),
}

impl RetryAfter {
    /// The absolute time the baseline's `RetryAfter()` would report.
    pub fn at(self, now: OffsetDateTime) -> OffsetDateTime {
        match self {
            Self::Seconds(seconds) => now + time::Duration::seconds(seconds),
            Self::Date(date) => date,
        }
    }
}

/// `client.RateLimitExceededError` vs any other failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingsError {
    RateLimitExceeded(RetryAfter),
    Other(String),
}

impl EmbeddingsError {
    pub fn is_rate_limit(&self) -> bool {
        matches!(self, Self::RateLimitExceeded(_))
    }
}

impl std::fmt::Display for EmbeddingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimitExceeded(_) => f.write_str("rate limit exceeded"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

/// `client.EmbeddingsClient`.
#[async_trait]
pub trait EmbeddingsClient: Send + Sync {
    /// `GetQueryEmbedding`.
    async fn get_query_embedding(
        &self,
        query: String,
    ) -> Result<EmbeddingsResults, EmbeddingsError>;

    /// `GetDocumentEmbeddings`.
    async fn get_document_embeddings(
        &self,
        documents: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsError>;

    /// `GetDimensions`.
    fn get_dimensions(&self) -> Result<usize, String>;

    /// `GetModelIdentifier`: `provider/name`.
    fn get_model_identifier(&self) -> String;
}

// ---------------------------------------------------------------------------
// HTTP transport seam
// ---------------------------------------------------------------------------

/// A single HTTP request as issued by a provider client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// A raw HTTP response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// The injectable HTTP execution seam.
#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String>;
}

/// Production transport (`httpcli.UncachedExternalClient` equivalent).
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|err| err.to_string())?;
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .body(request.body)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|err| err.to_string())?
            .to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

// ---------------------------------------------------------------------------
// Factory (`embed.NewEmbeddingsClient`)
// ---------------------------------------------------------------------------

/// `embed.NewEmbeddingsClient`.
pub fn new_client(
    config: &ProviderConfig,
    transport: Arc<dyn HttpTransport>,
) -> Result<Arc<dyn EmbeddingsClient>, String> {
    match config.provider {
        Provider::Sourcegraph => Ok(Arc::new(SourcegraphClient::new(
            transport,
            config.model.clone(),
            config.dimensions,
            config.endpoint.clone(),
            config.access_token.clone(),
        ))),
        Provider::OpenAI => Ok(Arc::new(OpenAiClient::new(
            transport,
            config.model.clone(),
            config.dimensions,
            config.endpoint.clone(),
            config.access_token.clone(),
        ))),
        Provider::AzureOpenAI => {
            if config.access_token.is_empty() {
                return Err(
                    "not ported: azure-openai without an access token (DefaultAzureCredential)"
                        .to_owned(),
                );
            }
            Ok(Arc::new(AzureOpenAiClient::new(
                transport,
                config.model.clone(),
                config.dimensions,
                config.endpoint.clone(),
                config.access_token.clone(),
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

pub(crate) fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub(crate) fn json_body<T: Serialize>(value: &T) -> Result<Vec<u8>, EmbeddingsError> {
    serde_json::to_vec(value)
        .map_err(|err| EmbeddingsError::Other(format!("marshaling request: {err}")))
}

/// The baseline's generic non-200 error:
/// `embeddings: %s %q: failed with status %d: %s` with the body capped at 1024
/// bytes (`io.LimitReader`).
pub(crate) fn http_error_message(method: &str, url: &str, status: u16, body: &[u8]) -> String {
    let body = String::from_utf8_lossy(&body[..body.len().min(1024)]);
    format!("embeddings: {method} {url:?}: failed with status {status}: {body}")
}

/// Parses Go's `time.RFC1123` header format ("Mon, 02 Jan 2006 15:04:05 GMT").
///
/// Go accepts any zone abbreviation and treats unknown zones as zero offset;
/// only the UTC-equivalent (offset 0) case is representable here, which is all
/// the baseline actually consumes (`RetryAfter()`).
pub fn parse_rfc1123(value: &str) -> Option<OffsetDateTime> {
    let value = value.trim();
    let (weekday, rest) = value.split_once(", ")?;
    if weekday.len() != 3 || !weekday.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }
    let mut parts = rest.split(' ');
    let day: u8 = parts.next()?.parse().ok()?;
    let month = month_from_abbreviation(parts.next()?)?;
    let year: i32 = parts.next()?.parse().ok()?;
    let time_part = parts.next()?;
    let zone = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    // Go's RFC1123 layout requires a zone abbreviation and computes the offset
    // from the local zone database; "GMT"/"UTC" are the zero-offset cases.
    if !zone.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }
    let mut clock = time_part.split(':');
    let hour: u8 = clock.next()?.parse().ok()?;
    let minute: u8 = clock.next()?.parse().ok()?;
    let second: u8 = clock.next()?.parse().ok()?;
    if clock.next().is_some() {
        return None;
    }
    let date = Date::from_calendar_date(year, month, day).ok()?;
    let time = Time::from_hms(hour, minute, second).ok()?;
    Some(PrimitiveDateTime::new(date, time).assume_utc())
}

fn month_from_abbreviation(value: &str) -> Option<Month> {
    let month = match value {
        "Jan" => Month::January,
        "Feb" => Month::February,
        "Mar" => Month::March,
        "Apr" => Month::April,
        "May" => Month::May,
        "Jun" => Month::June,
        "Jul" => Month::July,
        "Aug" => Month::August,
        "Sep" => Month::September,
        "Oct" => Month::October,
        "Nov" => Month::November,
        "Dec" => Month::December,
        _ => return None,
    };
    Some(month)
}

/// Canonical output for a parsed retry-after date:
/// `2006-01-02T15:04:05Z` (UTC).
pub fn format_utc_rfc3339_seconds(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

/// Go's `url.PathEscape` for a single path segment (`encodePathSegment`).
pub(crate) fn path_escape(segment: &str) -> String {
    let mut escaped = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || matches!(byte, b'$' | b'&' | b'+' | b':' | b'=' | b'@');
        if keep {
            escaped.push(char::from(byte));
        } else {
            escaped.push('%');
            escaped.push(char::from(HEX_UPPER[(byte >> 4) as usize]));
            escaped.push(char::from(HEX_UPPER[(byte & 0x0f) as usize]));
        }
    }
    escaped
}

const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_names_match_the_baseline() {
        assert_eq!(
            Provider::try_from_name("sourcegraph").unwrap(),
            Provider::Sourcegraph
        );
        assert_eq!(Provider::try_from_name("openai").unwrap(), Provider::OpenAI);
        assert_eq!(
            Provider::try_from_name("azure-openai").unwrap(),
            Provider::AzureOpenAI
        );
        assert_eq!(
            Provider::try_from_name("bogus").unwrap_err(),
            "invalid provider \"bogus\""
        );
    }

    #[test]
    fn path_escape_matches_go_for_common_model_names() {
        assert_eq!(path_escape("my-deploy"), "my-deploy");
        assert_eq!(path_escape("my deploy"), "my%20deploy");
        assert_eq!(path_escape("my/deploy"), "my%2Fdeploy");
        assert_eq!(path_escape("a+b"), "a+b");
        assert_eq!(path_escape("a,b;c?d"), "a%2Cb%3Bc%3Fd");
    }

    #[test]
    fn rfc1123_parsing_and_formatting() {
        let parsed = parse_rfc1123("Mon, 02 Jan 2006 15:04:05 GMT").expect("parse");
        assert_eq!(format_utc_rfc3339_seconds(parsed), "2006-01-02T15:04:05Z");
        assert!(parse_rfc1123("Mon, 02 Jan 2006 15:04:05").is_none());
        assert!(parse_rfc1123("not a date").is_none());
        assert_eq!(
            RetryAfter::Seconds(3).at(parsed),
            parsed + time::Duration::seconds(3)
        );
    }

    #[test]
    fn factory_selects_by_provider_and_rejects_tokenless_azure() {
        let transport: Arc<dyn HttpTransport> = Arc::new(ReqwestTransport::new());
        let base = ProviderConfig {
            provider: Provider::Sourcegraph,
            access_token: "tok".to_owned(),
            model: "m".to_owned(),
            endpoint: "http://example/v1/embeddings".to_owned(),
            dimensions: 4,
        };
        assert_eq!(
            new_client(&base, Arc::clone(&transport))
                .unwrap()
                .get_model_identifier(),
            "sourcegraph/m"
        );
        let openai = ProviderConfig {
            provider: Provider::OpenAI,
            ..base.clone()
        };
        assert_eq!(
            new_client(&openai, Arc::clone(&transport))
                .unwrap()
                .get_model_identifier(),
            "openai/m"
        );
        let azure = ProviderConfig {
            provider: Provider::AzureOpenAI,
            access_token: String::new(),
            ..base
        };
        assert!(new_client(&azure, transport)
            .err()
            .expect("tokenless azure is rejected")
            .contains("DefaultAzureCredential"));
    }
}
