#![forbid(unsafe_code)]

//! S3 HTTP routing (`s3_routes.go`).
//!
//! Mirrors Go's `serveS3` path/method dispatch and the individual S3 handlers:
//! request bodies stream straight into the backend, successful responses use
//! the XML bodies from [`crate::s3_xml`], and sentinel backend errors become
//! the same S3 error documents (with the same status codes) as the baseline.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{
    HeaderValue, ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE,
};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use tonic::body::BoxBody;

use crate::backend::{BackendError, ByteStream, MultipartBackend};
use crate::paths;
use crate::s3_xml;

const XML_CONTENT_TYPE: &str = "application/xml;charset=utf-8";
const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// Build the hyper handler consumed by `sg_http::serve_http`.
pub fn handler(backend: Arc<dyn MultipartBackend>) -> sg_http::HttpHandler {
    Arc::new(move |request: Request<Incoming>| {
        let backend = Arc::clone(&backend);
        Box::pin(async move { handle(backend, request).await })
    })
}

async fn handle(
    backend: Arc<dyn MultipartBackend>,
    request: Request<Incoming>,
) -> Response<BoxBody> {
    // `shared.go` answers this before any middleware; any method is accepted.
    if request.uri().path() == "/healthz" {
        return text_response(StatusCode::OK, "ok".to_owned());
    }

    let method = request.method().clone();
    let uri = request.uri().clone();
    let segments = paths::split_path(uri.path());
    let query = Query::parse(uri.query());
    let has_copy_source =
        header_str(request.headers(), "x-amz-copy-source").is_some_and(|value| !value.is_empty());

    let route = match route(&segments, &method, &query, has_copy_source) {
        Some(route) => route,
        None => {
            return plain_error_response(&format!("unsupported method: {method} request: {uri}"))
        }
    };

    dispatch(backend, route, request, &query).await
}

/// The route selected by `(path, method, query)`; a pure function so the
/// dispatch table can be unit tested without a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    ListObjects { bucket: String },
    CreateBucket { bucket: String },
    DeleteObjects { bucket: String },
    HeadObject { bucket: String, key: String },
    GetObject { bucket: String, key: String },
    PutObject { bucket: String, key: String },
    UploadPart { bucket: String, key: String },
    UploadPartCopy { bucket: String, key: String },
    CreateMultipartUpload { bucket: String, key: String },
    CompleteMultipartUpload { bucket: String, key: String },
    AbortMultipartUpload { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
}

fn route(
    segments: &[String],
    method: &Method,
    query: &Query,
    has_copy_source: bool,
) -> Option<Route> {
    match segments.len() {
        1 => {
            let bucket = segments[0].clone();
            if method == Method::GET {
                Some(Route::ListObjects { bucket })
            } else if method == Method::PUT {
                Some(Route::CreateBucket { bucket })
            } else if method == Method::POST && query.has("delete") {
                Some(Route::DeleteObjects { bucket })
            } else {
                None
            }
        }
        n if n >= 2 => {
            let bucket = segments[0].clone();
            // An S3 object key is everything after the bucket, and keys may
            // contain `/` (aws-sdk-s3 sends those separators literally in the
            // request path), so rejoin the remaining segments.
            let key = segments[1..].join("/");
            if method == Method::HEAD {
                Some(Route::HeadObject { bucket, key })
            } else if method == Method::GET {
                Some(Route::GetObject { bucket, key })
            } else if method == Method::PUT {
                if query.has("partNumber") {
                    if has_copy_source {
                        Some(Route::UploadPartCopy { bucket, key })
                    } else {
                        Some(Route::UploadPart { bucket, key })
                    }
                } else {
                    Some(Route::PutObject { bucket, key })
                }
            } else if method == Method::POST {
                if query.has("uploads") {
                    Some(Route::CreateMultipartUpload { bucket, key })
                } else {
                    Some(Route::CompleteMultipartUpload { bucket, key })
                }
            } else if method == Method::DELETE {
                if query.has("uploadId") {
                    Some(Route::AbortMultipartUpload { bucket, key })
                } else {
                    Some(Route::DeleteObject { bucket, key })
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

async fn dispatch(
    backend: Arc<dyn MultipartBackend>,
    route: Route,
    request: Request<Incoming>,
    query: &Query,
) -> Response<BoxBody> {
    match route {
        Route::ListObjects { bucket } => {
            let prefix = query.get("prefix").unwrap_or("");
            match backend.list_objects(&bucket, prefix).await {
                Ok(objects) => xml_response(
                    StatusCode::OK,
                    s3_xml::list_bucket_result(&bucket, &objects),
                ),
                Err(error @ BackendError::NoSuchBucket) => {
                    s3_error_response(&error, &bucket, StatusCode::CONFLICT)
                }
                Err(error) => plain_error_response(&format!("listObjects: {error}")),
            }
        }
        Route::CreateBucket { bucket } => {
            // Go compares `r.ContentLength != 0`; treat an unknown length as 0
            // exactly as the skeleton contract specifies.
            let content_length = request
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            if content_length != 0 {
                return plain_error_response(&format!(
                    "expected CreateBucket request to have content length 0: {} {}",
                    request.method(),
                    request.uri()
                ));
            }
            match backend.create_bucket(&bucket).await {
                Ok(()) => empty_response(StatusCode::OK),
                Err(error @ BackendError::BucketAlreadyExists) => {
                    s3_error_response(&error, &bucket, StatusCode::CONFLICT)
                }
                Err(error) => plain_error_response(&format!("createBucket: {error}")),
            }
        }
        Route::DeleteObjects { bucket } => {
            let body = match request.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(error) => {
                    return plain_error_response(&format!("decoding XML request: {error}"))
                }
            };
            let keys = match s3_xml::parse_delete_objects(&body) {
                Ok(keys) => keys,
                Err(error) => {
                    return plain_error_response(&format!("decoding XML request: {error}"))
                }
            };
            // Go ignores per-key errors (including NoSuchKey) and always succeeds.
            let _ = backend.delete_objects(&bucket, &keys).await;
            empty_response(StatusCode::OK)
        }
        Route::HeadObject { bucket, key } => match backend.head_object(&bucket, &key).await {
            Ok(metadata) => {
                let mut response = empty_response(StatusCode::OK);
                response
                    .headers_mut()
                    .insert(CONTENT_LENGTH, header_value(metadata.size));
                response
            }
            Err(error @ BackendError::NoSuchKey) => {
                s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
            }
            Err(error) => plain_error_response(&format!("getObject: {error}")),
        },
        Route::GetObject { bucket, key } => {
            let range = request
                .headers()
                .get(RANGE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            get_object(backend.as_ref(), &bucket, &key, range.as_deref()).await
        }
        Route::PutObject { bucket, key } => {
            let body = streaming_request_body(request.into_body());
            match backend.put_object(&bucket, &key, body).await {
                // Go sends no ETag; 200 with an empty body.
                Ok(_metadata) => empty_response(StatusCode::OK),
                Err(error @ BackendError::NoSuchBucket) => {
                    s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
                }
                Err(error) => plain_error_response(&format!("putObject: {error}")),
            }
        }
        Route::UploadPart { bucket, key } => {
            let part_number = match parse_part_number(query) {
                Ok(part_number) => part_number,
                Err(message) => return plain_error_response(&message),
            };
            let upload_id = query.get("uploadId").unwrap_or("").to_owned();
            let body = streaming_request_body(request.into_body());
            match backend
                .upload_part(&bucket, &key, &upload_id, part_number, body)
                .await
            {
                Ok(_metadata) => empty_response(StatusCode::OK),
                Err(error @ BackendError::NoSuchUpload) => {
                    s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
                }
                Err(error) => plain_error_response(&format!("uploadPart: {error}")),
            }
        }
        Route::UploadPartCopy { bucket, key } => {
            let copy_source = match header_str(request.headers(), "x-amz-copy-source") {
                Some(value) if !value.is_empty() => value.to_owned(),
                _ => return plain_error_response("expected header: x-amz-copy-source"),
            };
            let part_number = match parse_part_number(query) {
                Ok(part_number) => part_number,
                Err(message) => return plain_error_response(&message),
            };
            let upload_id = query.get("uploadId").unwrap_or("").to_owned();
            let (source_bucket, source_key) = match copy_source.split_once('/') {
                Some(parts) => parts,
                None => {
                    return plain_error_response(
                        "expected x-amz-copy-source header to have 2 components",
                    )
                }
            };
            if header_str(request.headers(), "x-amz-copy-source-range")
                .is_some_and(|value| !value.is_empty())
            {
                return plain_error_response("x-amz-copy-source-range is not supported");
            }
            match backend
                .upload_part_copy(
                    &bucket,
                    &key,
                    &upload_id,
                    part_number,
                    source_bucket,
                    source_key,
                )
                .await
            {
                Ok(metadata) => xml_response(
                    StatusCode::OK,
                    s3_xml::copy_part_result(metadata.last_modified),
                ),
                Err(error @ BackendError::NoSuchUpload) => {
                    s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
                }
                Err(error) => plain_error_response(&format!("uploadPart: {error}")),
            }
        }
        Route::CreateMultipartUpload { bucket, key } => {
            match query.get("uploads") {
                Some("") => {}
                Some(_) => {
                    return plain_error_response("?uploads query parameter must always be empty")
                }
                None => return plain_error_response("expected ?uploads= query parameter"),
            }
            match backend.create_upload(&bucket, &key).await {
                Ok(upload_id) => xml_response(
                    StatusCode::OK,
                    s3_xml::initiate_multipart_upload_result(&bucket, &key, &upload_id),
                ),
                Err(error) => plain_error_response(&format!("createUpload: {error}")),
            }
        }
        Route::CompleteMultipartUpload { bucket, key } => {
            // The Go handler never reads the completion request body, but it is
            // drained here so the connection can be reused.
            let _ = request.into_body().collect().await;
            let upload_id = query.get("uploadId").unwrap_or("").to_owned();
            match backend.complete_upload(&bucket, &key, &upload_id).await {
                Ok(()) => xml_response(
                    StatusCode::OK,
                    s3_xml::complete_multipart_upload_result(&bucket, &key),
                ),
                Err(error @ BackendError::NoSuchUpload)
                | Err(error @ BackendError::InvalidPartOrder) => {
                    s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
                }
                Err(error) => plain_error_response(&format!("completeUpload: {error}")),
            }
        }
        Route::AbortMultipartUpload { bucket, key } => {
            let upload_id = query.get("uploadId").unwrap_or("");
            if upload_id.is_empty() {
                return plain_error_response("expected ?uploadId query parameter");
            }
            match backend.abort_upload(&bucket, &key, upload_id).await {
                Ok(()) => empty_response(StatusCode::OK),
                Err(error @ BackendError::NoSuchUpload) => {
                    s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
                }
                Err(error) => plain_error_response(&format!("abortUpload: {error}")),
            }
        }
        Route::DeleteObject { bucket, key } => match backend.delete_object(&bucket, &key).await {
            Ok(()) => empty_response(StatusCode::OK),
            Err(error @ BackendError::NoSuchKey) => {
                s3_error_response(&error, &bucket, StatusCode::NOT_FOUND)
            }
            Err(error) => plain_error_response(&format!("deleteObject: {error}")),
        },
    }
}

async fn get_object(
    backend: &dyn MultipartBackend,
    bucket: &str,
    key: &str,
    range_header: Option<&str>,
) -> Response<BoxBody> {
    let reader = match backend.get_object(bucket, key).await {
        Ok(reader) => reader,
        Err(error @ BackendError::NoSuchKey) => {
            return s3_error_response(&error, bucket, StatusCode::NOT_FOUND)
        }
        Err(error) => return plain_error_response(&format!("getObject: {error}")),
    };

    let mut file = reader.file;
    let size = reader.size;

    match range_header.and_then(|value| parse_range(value, size)) {
        Some(RangeSpec::Unsatisfiable) => {
            let mut response = empty_response(StatusCode::RANGE_NOT_SATISFIABLE);
            response
                .headers_mut()
                .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            response
                .headers_mut()
                .insert(CONTENT_RANGE, header_value(format!("bytes */{size}")));
            response
        }
        Some(RangeSpec::Satisfiable { start, end }) => {
            if let Err(error) = file.seek(io::SeekFrom::Start(start)).await {
                return plain_error_response(&format!("seeking object: {error}"));
            }
            let length = end - start + 1;
            let mut response = Response::new(streaming_body(file.take(length)));
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            let headers = response.headers_mut();
            headers.insert(CONTENT_LENGTH, header_value(length));
            headers.insert(
                CONTENT_RANGE,
                header_value(format!("bytes {start}-{end}/{size}")),
            );
            headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            response
        }
        None => {
            let mut response = Response::new(streaming_body(file));
            let headers = response.headers_mut();
            headers.insert(CONTENT_LENGTH, header_value(size));
            headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            response
        }
    }
}

/// A `Range: bytes=...` request, resolved against the object size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeSpec {
    Satisfiable { start: u64, end: u64 },
    Unsatisfiable,
}

/// Parse the single-range forms Go's S3 clients emit: `start-`, `start-end`,
/// and `-suffix`. Returns `None` for an unusable header (serve the full body),
/// matching the baseline's lack of range handling for malformed values.
fn parse_range(value: &str, size: u64) -> Option<RangeSpec> {
    let rest = value.trim().strip_prefix("bytes=")?;
    if rest.contains(',') {
        return Some(RangeSpec::Unsatisfiable);
    }
    let (start, end) = rest.split_once('-')?;
    let start = start.trim();
    let end = end.trim();

    if start.is_empty() {
        let suffix: u64 = end.parse().ok()?;
        if suffix == 0 || size == 0 {
            return Some(RangeSpec::Unsatisfiable);
        }
        let start = size.saturating_sub(suffix);
        return Some(RangeSpec::Satisfiable {
            start,
            end: size - 1,
        });
    }

    let start: u64 = start.parse().ok()?;
    if start >= size {
        return Some(RangeSpec::Unsatisfiable);
    }
    if end.is_empty() {
        return Some(RangeSpec::Satisfiable {
            start,
            end: size - 1,
        });
    }
    let end: u64 = end.parse().ok()?;
    if end < start {
        return Some(RangeSpec::Unsatisfiable);
    }
    Some(RangeSpec::Satisfiable {
        start,
        end: end.min(size - 1),
    })
}

fn parse_part_number(query: &Query) -> Result<i32, String> {
    query
        .get("partNumber")
        .unwrap_or("")
        .parse::<i32>()
        .map_err(|_| "partNumber query parameter must be an integer".to_owned())
}

/// Go `url.Values`: parsed once from the raw query string, preserving order.
#[derive(Debug, Default)]
struct Query {
    pairs: Vec<(String, String)>,
}

impl Query {
    fn parse(query: Option<&str>) -> Self {
        let pairs = url::form_urlencoded::parse(query.unwrap_or("").as_bytes())
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        Self { pairs }
    }

    /// `url.Values.Has`: presence, even with an empty value.
    fn has(&self, name: &str) -> bool {
        self.pairs.iter().any(|(key, _)| key == name)
    }

    /// `url.Values.Get`: the first value.
    fn get(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

fn streaming_request_body(body: Incoming) -> ByteStream {
    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(io::Error::other));
    Box::pin(stream)
}

fn streaming_body<R>(reader: R) -> BoxBody
where
    R: tokio::io::AsyncRead + Send + 'static,
{
    let stream = ReaderStream::new(reader).map(|result| result.map(Frame::data));
    StreamBody::new(stream)
        .map_err(|error: io::Error| tonic::Status::internal(error.to_string()))
        .boxed_unsync()
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn header_value(value: impl ToString) -> HeaderValue {
    HeaderValue::from_str(&value.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0"))
}

fn body_from_bytes(bytes: Vec<u8>) -> BoxBody {
    BoxBody::new(Full::new(Bytes::from(bytes)).map_err(|error| match error {}))
}

fn bytes_response(
    status: StatusCode,
    content_type: &'static str,
    bytes: Vec<u8>,
) -> Response<BoxBody> {
    let mut response = Response::new(body_from_bytes(bytes));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(body_from_bytes(Vec::new()));
    *response.status_mut() = status;
    response
}

fn xml_response(status: StatusCode, body: String) -> Response<BoxBody> {
    bytes_response(status, XML_CONTENT_TYPE, body.into_bytes())
}

fn text_response(status: StatusCode, body: String) -> Response<BoxBody> {
    bytes_response(status, TEXT_CONTENT_TYPE, body.into_bytes())
}

fn s3_error_response(error: &BackendError, bucket: &str, status: StatusCode) -> Response<BoxBody> {
    xml_response(
        status,
        s3_xml::error_documents(error.code(), &error.to_string(), bucket),
    )
}

/// Go `ServeHTTP`'s error branch: 500 with `blobstore: error: <message>`.
fn plain_error_response(message: &str) -> Response<BoxBody> {
    text_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("blobstore: error: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segments(path: &str) -> Vec<String> {
        paths::split_path(path)
    }

    fn route_of(path: &str, method: Method, query: &str, copy_source: bool) -> Option<Route> {
        route(
            &segments(path),
            &method,
            &Query::parse(Some(query)),
            copy_source,
        )
    }

    #[test]
    fn bucket_routes() {
        assert_eq!(
            route_of("/bucket", Method::GET, "", false),
            Some(Route::ListObjects {
                bucket: "bucket".into()
            })
        );
        assert_eq!(
            route_of("/bucket", Method::PUT, "", false),
            Some(Route::CreateBucket {
                bucket: "bucket".into()
            })
        );
        assert_eq!(
            route_of("/bucket", Method::POST, "delete", false),
            Some(Route::DeleteObjects {
                bucket: "bucket".into()
            })
        );
        assert_eq!(route_of("/bucket", Method::POST, "uploads", false), None);
        assert_eq!(route_of("/bucket", Method::DELETE, "", false), None);
        assert_eq!(route_of("/bucket", Method::HEAD, "", false), None);
    }

    #[test]
    fn object_routes() {
        let head = Some(Route::HeadObject {
            bucket: "b".into(),
            key: "k".into(),
        });
        assert_eq!(route_of("/b/k", Method::HEAD, "", false), head);
        assert_eq!(
            route_of("/b/k", Method::GET, "", false),
            Some(Route::GetObject {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::PUT, "", false),
            Some(Route::PutObject {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::PUT, "partNumber=1&uploadId=u", false),
            Some(Route::UploadPart {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::PUT, "partNumber=1&uploadId=u", true),
            Some(Route::UploadPartCopy {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::POST, "uploads=", false),
            Some(Route::CreateMultipartUpload {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::POST, "uploadId=u", false),
            Some(Route::CompleteMultipartUpload {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::DELETE, "uploadId=u", false),
            Some(Route::AbortMultipartUpload {
                bucket: "b".into(),
                key: "k".into()
            })
        );
        assert_eq!(
            route_of("/b/k", Method::DELETE, "", false),
            Some(Route::DeleteObject {
                bucket: "b".into(),
                key: "k".into()
            })
        );
    }

    #[test]
    fn keys_may_contain_slashes() {
        assert_eq!(
            route_of("/bucket/dir/obj", Method::GET, "", false),
            Some(Route::GetObject {
                bucket: "bucket".into(),
                key: "dir/obj".into()
            })
        );
        assert_eq!(
            route_of("/bucket/dir/obj", Method::PUT, "", false),
            Some(Route::PutObject {
                bucket: "bucket".into(),
                key: "dir/obj".into()
            })
        );
    }

    #[test]
    fn unsupported_shapes() {
        assert_eq!(route_of("/", Method::GET, "", false), None);
        assert_eq!(route_of("/a/b", Method::PATCH, "", false), None);
        assert_eq!(route_of("/a/b", Method::OPTIONS, "", false), None);
    }

    #[test]
    fn query_presence_and_first_value() {
        let query = Query::parse(Some("delete&uploads=&prefix=a%20b&prefix=second"));
        assert!(query.has("delete"));
        assert!(query.has("uploads"));
        assert_eq!(query.get("uploads"), Some(""));
        assert_eq!(query.get("prefix"), Some("a b"));
        assert!(!query.has("uploadId"));
        assert_eq!(query.get("uploadId"), None);

        let implied = Query::parse(Some("delete&uploads"));
        assert!(implied.has("delete"));
        assert_eq!(implied.get("uploads"), Some(""));
    }

    #[test]
    fn range_forms() {
        use RangeSpec::*;

        assert_eq!(
            parse_range("bytes=0-4", 10),
            Some(Satisfiable { start: 0, end: 4 })
        );
        assert_eq!(
            parse_range("bytes=5-", 10),
            Some(Satisfiable { start: 5, end: 9 })
        );
        assert_eq!(
            parse_range("bytes=-3", 10),
            Some(Satisfiable { start: 7, end: 9 })
        );
        assert_eq!(
            parse_range("bytes=8-100", 10),
            Some(Satisfiable { start: 8, end: 9 })
        );
        assert_eq!(parse_range("bytes=10-", 10), Some(Unsatisfiable));
        assert_eq!(parse_range("bytes=4-2", 10), Some(Unsatisfiable));
        assert_eq!(parse_range("bytes=-0", 10), Some(Unsatisfiable));
        assert_eq!(parse_range("bytes=0-1,3-4", 10), Some(Unsatisfiable));
        assert_eq!(parse_range("items=0-4", 10), None);
    }

    #[test]
    fn part_number_parsing() {
        assert_eq!(
            parse_part_number(&Query::parse(Some("partNumber=3"))),
            Ok(3)
        );
        assert!(parse_part_number(&Query::parse(Some("uploadId=u"))).is_err());
        assert!(parse_part_number(&Query::parse(Some("partNumber=abc"))).is_err());
    }
}
