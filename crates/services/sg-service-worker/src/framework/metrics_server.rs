#![forbid(unsafe_code)]

//! `internal/metrics/store` + `promhttp.HandlerFor` + `internal/httpserver`.
//!
//! The executors metrics server exposes the metrics that executor instances
//! ingest into the cache Redis under `executors:<instance>`:
//!
//! * [`DistributedStore::gather`] is the baseline's `Gather`: `KEYS` over the
//!   prefix, one `MGET`, then a per-blob text decode (`expfmt.Decoder`) into
//!   metric families. Every failure is wrapped exactly like the baseline
//!   (`listing entries from redis`, `retrieving blobs from redis`, `decoding
//!   metrics data`);
//! * [`DistributedStore::ingest`] is the write side the
//!   `executors-multiqueue-metrics-reporter` job will use (encode each family
//!   with `expfmt`, then `SETEX` with the 30 second expiry);
//! * [`handler`] reproduces `promhttp.HandlerFor(..., HandlerOpts{})`: a
//!   gather error is a `500` with
//!   `An error has occurred while serving metrics:\n\n<err>\n`, a success is
//!   `text/plain; version=0.0.4; charset=utf-8; escaping=values` (gzip when
//!   the client accepts it);
//! * [`MetricsServerRoutine`] is `httpserver.NewFromAddr`: bind, then serve
//!   until the process context is cancelled. A bind or serve error makes the
//!   process exit (see `BackgroundRoutine::spawn`).
//!
//! Errata (docs/modules/06_worker.md §22): the store dials Redis lazily like
//! `redispool.Cache`, but a cached connection manager keeps retrying on
//! demand; nested compression is negotiated only from the first
//! `Accept-Encoding` header; the decoded family order is deterministic.

use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use flate2::write::GzEncoder;
use http::header::{HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use sg_metrics::textfmt::{self, MetricFamily};
use sg_redis::KeyValue;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use tonic::body::BoxBody;

use super::ServerRoutine;

/// `store.DefaultMetricsExpiry`.
pub const DEFAULT_METRICS_EXPIRY: i64 = 30;

/// `httpserver.server.Name()`.
pub const SERVER_NAME: &str = "HTTP server";

/// `distributedStore`: text blobs in `redispool.Cache` under
/// `<prefix><instance>`.
pub struct DistributedStore {
    prefix: String,
    expiry: i64,
    endpoint: String,
    /// `redispool.Cache` dials lazily; the first call that needs a connection
    /// creates it, and failures are retried on the next call.
    kv: OnceCell<KeyValue>,
}

impl DistributedStore {
    /// `store.NewDistributedStore`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            expiry: DEFAULT_METRICS_EXPIRY,
            endpoint: sg_redis::cache_endpoint(),
            kv: OnceCell::new(),
        }
    }

    /// The object-store prefix (`executors:` for the executors metrics).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn expiry(&self) -> i64 {
        self.expiry
    }

    async fn kv(&self) -> Result<&KeyValue, String> {
        self.kv
            .get_or_try_init(|| async {
                KeyValue::connect(&self.endpoint)
                    .await
                    .map_err(|err| err.to_string())
            })
            .await
    }

    /// `distributedStore.Gather`.
    pub async fn gather(&self) -> Result<Vec<MetricFamily>, String> {
        let kv = self
            .kv()
            .await
            .map_err(|err| format!("listing entries from redis: {err}"))?;
        let keys = kv
            .keys(&format!("{}*", self.prefix))
            .await
            .map_err(|err| format!("listing entries from redis: {err}"))?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let blobs = kv
            .mget(&keys)
            .await
            .map_err(|err| format!("retrieving blobs from redis: {err}"))?;
        // `MGet(keys).Strings()` fails on a nil reply (a key that expired
        // between KEYS and MGET).
        let blobs = blobs
            .into_iter()
            .collect::<Option<Vec<Vec<u8>>>>()
            .ok_or_else(|| "retrieving blobs from redis: redigo: nil returned".to_owned())?;

        let mut families = Vec::new();
        for blob in blobs {
            // `strings.NewReader` around the stored blob; the encoder only
            // ever writes UTF-8.
            let text = String::from_utf8_lossy(&blob);
            let decoded = textfmt::parse_text(&text)
                .map_err(|err| format!("decoding metrics data: {err}"))?;
            families.extend(decoded);
        }
        Ok(families)
    }

    /// `distributedStore.Ingest`.
    pub async fn ingest(&self, instance: &str, families: &[MetricFamily]) -> Result<(), String> {
        let mut encoded = String::new();
        for family in families {
            encoded.push_str(
                &textfmt::encode_family(family)
                    .map_err(|err| format!("encoding metric family: {err}"))?,
            );
        }

        let kv = self
            .kv()
            .await
            .map_err(|err| format!("writing metrics blob to redis: {err}"))?;
        kv.set_ex(
            &format!("{}{}", self.prefix, instance),
            self.expiry,
            encoded,
        )
        .await
        .map_err(|err| format!("writing metrics blob to redis: {err}"))
    }
}

/// `promhttp.HandlerFor(gatherer, HandlerOpts{})`.
pub fn handler(store: Arc<DistributedStore>) -> sg_http::HttpHandler {
    Arc::new(move |req: Request<Incoming>| {
        let store = Arc::clone(&store);
        Box::pin(async move { serve(store, req).await })
    })
}

async fn serve(store: Arc<DistributedStore>, req: Request<Incoming>) -> Response<BoxBody> {
    match store.gather().await {
        Err(err) => http_error(&err),
        Ok(families) => success_response(&req, &families),
    }
}

/// `writeError`'s `httpError` + `MetricFamilyToText` loop.
fn success_response<B>(req: &Request<B>, families: &[MetricFamily]) -> Response<BoxBody> {
    let mut body = String::new();
    for family in families {
        match textfmt::encode_family(family) {
            Ok(text) => body.push_str(&text),
            Err(err) => {
                // `HTTPErrorOnError` cannot send a new status once the body
                // started; promhttp aborts the response instead.
                tracing::error!(error = %err, "error encoding and sending metric family");
                break;
            }
        }
    }
    text_response(StatusCode::OK, req.headers(), body)
}

/// `httpError`: 500 with the promhttp error text, never compressed.
fn http_error(err: &str) -> Response<BoxBody> {
    let body = format!("An error has occurred while serving metrics:\n\n{err}\n");
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        http::header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn text_response(status: StatusCode, headers: &HeaderMap, body: String) -> Response<BoxBody> {
    let (body, gzipped) = if gzip_accepted(headers) {
        match gzip(&body) {
            Some(compressed) => (compressed, true),
            None => (body.into_bytes(), false),
        }
    } else {
        (body.into_bytes(), false)
    };

    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(sg_metrics::CONTENT_TYPE),
    );
    if gzipped {
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    response
}

/// `promhttp.gzipAccepted`.
fn gzip_accepted(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(ACCEPT_ENCODING).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    value.split(',').any(|part| {
        let part = part.trim();
        part == "gzip" || part.starts_with("gzip;")
    })
}

fn gzip(body: &str) -> Option<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body.as_bytes()).ok()?;
    encoder.finish().ok()
}

/// `httpserver.NewFromAddr(addr, &http.Server{Handler: handler})`.
pub struct MetricsServerRoutine {
    addr: String,
    handler: sg_http::HttpHandler,
}

impl MetricsServerRoutine {
    pub fn new(addr: impl Into<String>, handler: sg_http::HttpHandler) -> Self {
        Self {
            addr: addr.into(),
            handler,
        }
    }

    /// The address `NewFromAddr` was given (`host:port`).
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

#[async_trait]
impl ServerRoutine for MetricsServerRoutine {
    fn name(&self) -> &'static str {
        SERVER_NAME
    }

    /// `server.Start`: bind, then `Serve` until `Stop` (here: context
    /// cancellation) shuts it down.
    async fn start(&self, shutdown: CancellationToken) -> Result<(), String> {
        let listener = sg_http::listen(&self.addr)
            .await
            .map_err(|err| format!("Failed to create listener: {err}"))?;
        sg_http::serve_http(listener, Arc::clone(&self.handler), shutdown)
            .await
            .map_err(|err| format!("Failed to start server: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    use textfmt::{LabelPair, Metric, MetricType};

    async fn body_of(response: Response<BoxBody>) -> Vec<u8> {
        response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec()
    }

    fn family() -> MetricFamily {
        MetricFamily {
            name: "src_worker_test_total".to_owned(),
            help: Some("a test counter".to_owned()),
            metric_type: MetricType::Counter,
            metrics: vec![Metric {
                labels: vec![LabelPair {
                    name: "path".to_owned(),
                    value: "a".to_owned(),
                }],
                counter: Some(3.0),
                ..Metric::default()
            }],
        }
    }

    #[tokio::test]
    async fn success_is_the_negotiated_text_format() {
        let req = Request::builder().body(()).expect("request");
        let response = success_response(&req, &[family()]);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).expect("content type"),
            "text/plain; version=0.0.4; charset=utf-8; escaping=values"
        );
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert_eq!(
            body_of(response).await,
            b"# HELP src_worker_test_total a test counter\n\
# TYPE src_worker_test_total counter\n\
src_worker_test_total{path=\"a\"} 3\n"
        );
    }

    #[tokio::test]
    async fn gzip_is_used_when_accepted() {
        let req = Request::builder()
            .header(ACCEPT_ENCODING, "deflate, gzip;q=1.0")
            .body(())
            .expect("request");
        let response = success_response(&req, &[family()]);
        assert_eq!(response.headers().get(CONTENT_ENCODING).unwrap(), "gzip");
        let compressed = body_of(response).await;
        let mut decoder = flate2::read::GzDecoder::new(compressed.as_slice());
        let mut text = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut text).expect("gunzip");
        assert_eq!(
            text,
            "# HELP src_worker_test_total a test counter\n\
# TYPE src_worker_test_total counter\n\
src_worker_test_total{path=\"a\"} 3\n"
        );

        // `gzip;q=0` still counts (`strings.HasPrefix(part, "gzip;")`).
        assert!(gzip_accepted(
            Request::builder()
                .header(ACCEPT_ENCODING, "gzip;q=0")
                .body(())
                .unwrap()
                .headers()
        ));
        assert!(!gzip_accepted(
            Request::builder()
                .header(ACCEPT_ENCODING, "br, deflate")
                .body(())
                .unwrap()
                .headers()
        ));
    }

    #[tokio::test]
    async fn gather_errors_are_http_errors() {
        let response = http_error("listing entries from redis: boom");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).expect("content type"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response
                .headers()
                .get(http::header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
        assert_eq!(
            body_of(response).await,
            b"An error has occurred while serving metrics:\n\nlisting entries from redis: boom\n"
        );
    }

    #[test]
    fn store_defaults_match_the_baseline() {
        let store = DistributedStore::new("executors:");
        assert_eq!(store.prefix(), "executors:");
        assert_eq!(store.expiry(), 30);
        assert_eq!(DEFAULT_METRICS_EXPIRY, 30);
        assert_eq!(SERVER_NAME, "HTTP server");
    }
}
