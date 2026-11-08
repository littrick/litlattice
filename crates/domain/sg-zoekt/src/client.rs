#![forbid(unsafe_code)]

//! gRPC client for `zoekt.webserver.v1.WebserverService`.
//!
//! Mirrors the zoekt-side client Sourcegraph's searcher uses
//! (`github.com/sourcegraph/zoekt/grpc`): a 128 MiB max receive size,
//! `context.Canceled`/`context.DeadlineExceeded` mapped from gRPC status codes.

use std::fmt;

use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

use crate::proto::{self, webserver_service_client::WebserverServiceClient};
use crate::query::{to_proto as query_to_proto, Query};
use crate::types::{ListOptions, ListResponse, SearchChunk, SearchOptions};

/// `grpc.Client`'s max receive message size in the baseline.
pub const MAX_RECV_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Failure talking to a zoekt-webserver.
#[derive(Debug)]
pub enum Error {
    /// The caller cancelled the request at the client side.
    Canceled,
    /// The RPC hit its deadline.
    DeadlineExceeded,
    /// Any other gRPC status.
    Status(Box<Status>),
    /// The endpoint could not be parsed.
    InvalidEndpoint(String),
    /// The connection could not be established.
    Transport(String),
}

impl Error {
    fn from_status(status: Status) -> Self {
        match status.code() {
            tonic::Code::Cancelled => Self::Canceled,
            tonic::Code::DeadlineExceeded => Self::DeadlineExceeded,
            _ => Self::Status(Box::new(status)),
        }
    }

    /// Whether the failure was a cancellation (baseline `context.Canceled`).
    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled)
    }

    /// Whether the failure was a deadline (baseline
    /// `context.DeadlineExceeded`).
    pub fn is_deadline_exceeded(&self) -> bool {
        matches!(self, Self::DeadlineExceeded)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Canceled => f.write_str("context canceled"),
            Self::DeadlineExceeded => f.write_str("context deadline exceeded"),
            Self::Status(status) => write!(f, "{}", status.message()),
            Self::InvalidEndpoint(message) | Self::Transport(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

/// A zoekt-webserver client.
#[derive(Debug, Clone)]
pub struct Client {
    inner: WebserverServiceClient<Channel>,
}

impl Client {
    /// Dial an endpoint eagerly. `endpoint` may be a bare `host:port` or a
    /// URI with an `http://` scheme.
    pub async fn connect(endpoint: &str) -> Result<Self, Error> {
        let channel = build_endpoint(endpoint)?
            .connect()
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(Self::new(channel))
    }

    /// Dial lazily: connection failures surface on the first RPC.
    pub fn connect_lazy(endpoint: &str) -> Result<Self, Error> {
        Ok(Self::new(build_endpoint(endpoint)?.connect_lazy()))
    }

    pub fn new(channel: Channel) -> Self {
        let inner =
            WebserverServiceClient::new(channel).max_decoding_message_size(MAX_RECV_MESSAGE_SIZE);
        Self { inner }
    }

    /// `zoekt.Streamer.List`.
    pub async fn list(&self, query: &Query, options: ListOptions) -> Result<ListResponse, Error> {
        let request = proto::ListRequest {
            query: Some(query_to_proto(query)),
            opts: Some(options.to_proto()),
        };
        let response = self
            .inner
            .clone()
            .list(request)
            .await
            .map_err(Error::from_status)?;
        Ok(ListResponse::from_proto(response.into_inner()))
    }

    /// `zoekt.Streamer.StreamSearch`.
    ///
    /// Each response chunk is handed to `on_chunk` as it arrives. Returning
    /// `Err(Error::Canceled)` means the caller's token fired (either the
    /// request was cancelled upstream or the callback called
    /// [`CancellationToken::cancel`]).
    pub async fn stream_search<F>(
        &self,
        query: &Query,
        options: &SearchOptions,
        cancel: &CancellationToken,
        mut on_chunk: F,
    ) -> Result<(), Error>
    where
        F: FnMut(SearchChunk),
    {
        let request = proto::StreamSearchRequest {
            request: Some(proto::SearchRequest {
                query: Some(query_to_proto(query)),
                opts: Some(options.to_proto()),
            }),
        };

        let mut stream = self
            .inner
            .clone()
            .stream_search(request)
            .await
            .map_err(Error::from_status)?
            .into_inner();

        loop {
            let message = tokio::select! {
                () = cancel.cancelled() => return Err(Error::Canceled),
                message = stream.message() => message.map_err(Error::from_status)?,
            };
            let Some(response) = message else {
                return Ok(());
            };
            if let Some(chunk) = response.response_chunk {
                on_chunk(SearchChunk::from_proto(chunk));
            }
        }
    }
}

fn build_endpoint(endpoint: &str) -> Result<Endpoint, Error> {
    let uri = if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    Endpoint::from_shared(uri.clone())
        .map_err(|err| Error::InvalidEndpoint(format!("invalid zoekt endpoint {uri:?}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bare_and_scheme_endpoints() {
        assert!(build_endpoint("127.0.0.1:6070").is_ok());
        assert!(build_endpoint("http://127.0.0.1:6070").is_ok());
        assert!(build_endpoint("https://zoekt.example.com").is_ok());
    }

    #[test]
    fn maps_status_codes_like_the_baseline() {
        assert!(Error::from_status(Status::cancelled("x")).is_canceled());
        assert!(Error::from_status(Status::deadline_exceeded("x")).is_deadline_exceeded());
        assert!(matches!(
            Error::from_status(Status::unavailable("x")),
            Error::Status(_)
        ));
    }
}
