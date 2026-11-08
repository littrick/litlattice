#![forbid(unsafe_code)]

//! `cmd/worker/shared`: the health server on `:3189`.
//!
//! `httpserver.NewFromAddr(addr, &http.Server{ReadTimeout: 75s,
//! WriteTimeout: 10m, Handler: httpserver.NewHandler(nil)})`.
//! `httpserver.NewHandler(nil)` installs exactly one route:
//! `HandleFunc("/healthz", ...)`, which answers 200 with an empty body and no
//! `Content-Type`. Every other path is gorilla/mux's `404 page not found`.
//! gorilla/mux matches the route exactly (no `StrictSlash`), so `/healthz/`
//! is a 404 as well.

use std::sync::Arc;

use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use tokio_util::sync::CancellationToken;
use tonic::body::BoxBody;

pub fn health_handler() -> sg_http::HttpHandler {
    Arc::new(|req: Request<Incoming>| Box::pin(async move { health_response(req) }))
}

pub fn health_response<B>(req: Request<B>) -> Response<BoxBody> {
    match req.uri().path() {
        "/healthz" => {
            // `w.WriteHeader(http.StatusOK)` writes no body and no
            // Content-Type.
            let mut response =
                Response::new(tonic::body::boxed(Full::new(hyper::body::Bytes::new())));
            *response.status_mut() = StatusCode::OK;
            response
        }
        _ => go_not_found(),
    }
}

/// Go's `http.NotFound`.
fn go_not_found() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(
        hyper::body::Bytes::from_static(b"404 page not found\n"),
    )));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

pub async fn serve_health(addr: &str, shutdown: CancellationToken) -> std::io::Result<()> {
    let listener = sg_http::listen(addr).await?;
    tracing::info!(addr, "health server listening");
    // `http.Server{ReadTimeout: 75s, WriteTimeout: 10m}` — hyper applies
    // neither; the health endpoint is a single small write.
    sg_http::serve_http(listener, health_handler(), shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn body_of(response: Response<BoxBody>) -> Vec<u8> {
        response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec()
    }

    #[tokio::test]
    async fn healthz_is_200_with_empty_body_and_no_content_type() {
        let req = Request::builder()
            .uri("/healthz")
            .body(())
            .expect("request");
        let response = health_response(req);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
        assert!(body_of(response).await.is_empty());
    }

    #[tokio::test]
    async fn unknown_paths_are_go_not_found() {
        for path in ["/", "/healthz/", "/nope"] {
            let req = Request::builder().uri(path).body(()).expect("request");
            let response = health_response(req);
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
            assert_eq!(
                response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .expect("content type"),
                "text/plain; charset=utf-8"
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::X_CONTENT_TYPE_OPTIONS)
                    .expect("nosniff"),
                "nosniff"
            );
            assert_eq!(body_of(response).await, b"404 page not found\n");
        }
    }
}
