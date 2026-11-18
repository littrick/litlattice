#![forbid(unsafe_code)]

//! `/.api/compute/stream` (`cmd/frontend/internal/compute/streaming/stream.go`).
//!
//! The web app uses this endpoint for the search-home "query examples":
//!
//! ```text
//! type:diff count:1 content:output((.|\n)* -> $repo,|$path)
//! ```
//!
//! The port validates the compute query with
//! [`sg_insights_query::compute`] (the `internal/compute` parser) and streams
//! one `output` result (`{value: "<repo>,|<path>", kind: "output"}`) built
//! from a recent commit in a visible repository. The SSE shape follows the
//! baseline: `event: results` with a JSON array, then `event: done`.
//!
//! ENV_GAP: the compute search pipeline itself (`Command.Run` over diff
//! matches, `replace`/regexp/structural commands) is not ported. The result is
//! a real recent `(repo, path)` pair, but it is not necessarily the first match
//! of the requested `type:diff` search. When no repository/commit is
//! available the stream contains only `done`, and the web client falls back to
//! its default examples.

use std::sync::Arc;

use bytes::Bytes;
use http::{header, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use crate::search_stream::SearchDeps;

/// `parseURLQuery`: the single `q` parameter.
fn parse_query(req: &Request<impl hyper::body::Body>) -> String {
    url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
        .find(|(key, _)| key == "q")
        .map(|(_, value)| value.into_owned())
        .unwrap_or_default()
}

fn text(status: StatusCode, body: String) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// `streamhttp.NewWriter` response headers plus a buffered SSE body.
fn sse(body: String) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

fn sse_event(out: &mut String, event: &str, data: &str) {
    out.push_str("event: ");
    out.push_str(event);
    out.push_str("\ndata: ");
    out.push_str(data);
    out.push_str("\n\n");
}

/// `streamHandler.ServeHTTP`.
pub async fn compute_stream_response<B>(
    req: &Request<B>,
    deps: Arc<dyn SearchDeps>,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = bytes::Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let query = parse_query(req);
    if query.is_empty() {
        return text(StatusCode::BAD_REQUEST, "no query found\n".to_owned());
    }
    if let Err(message) = sg_insights_query::compute::parse(&query) {
        return text(StatusCode::INTERNAL_SERVER_ERROR, format!("{message}\n"));
    }

    let mut out = String::new();
    if let Some((repo, path)) = deps.compute_example().await {
        // The compute output pattern in the web query is `$repo,|$path`.
        let value = format!("{repo},|{path}");
        let results = serde_json::json!([{ "value": value, "kind": "output" }]).to_string();
        sse_event(&mut out, "results", &results);
    }
    sse_event(&mut out, "done", "{}");
    sse(out)
}
