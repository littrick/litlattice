#![forbid(unsafe_code)]

//! Port of `internal/tenant/externalhttp.go`: the external tenant middleware.
//!
//! The production mapper (`cmd/frontend/internal/cli/http.go:125-128`)
//! hard-codes every host to tenant 1. The middleware writes its 400/500
//! responses with `http.Error` semantics and otherwise stores the tenant in
//! the request context, which Rust models as a request extension.

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

/// The tenant id, mirroring `tenant.Tenant`'s unexported `_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TenantId(pub i64);

/// `http.go:127`: all tenants are hard-coded to tenant 1.
pub const PRODUCTION_TENANT_ID: i64 = 1;

/// The two error categories `externalhttp.go` distinguishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantLookupError {
    /// `errcode.IsNotFound(err)`.
    NotFound,
    /// Any other mapper error.
    Other,
}

/// The production mapper: `func(context.Context, string) (int, error) { return 1, nil }`.
pub fn fixed_tenant_one(_host: &str) -> Result<i64, TenantLookupError> {
    Ok(PRODUCTION_TENANT_ID)
}

/// `extractHost`: everything before the last `:`, which only works for
/// `host:port` strings (not IPv6), exactly like the baseline comment says.
pub fn extract_host(host_port: &str) -> &str {
    match host_port.rfind(':') {
        Some(colon) => &host_port[..colon],
        None => host_port,
    }
}

/// Go's `http.Request.Host`: the `Host` header, or the URI authority for
/// HTTP/2 requests.
pub fn request_host<B>(req: &Request<B>) -> String {
    if let Some(host) = req
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    {
        return host.to_owned();
    }
    req.uri()
        .authority()
        .map(|authority| authority.as_str().to_owned())
        .unwrap_or_default()
}

/// `ExternalTenantFromHostnameMiddleware` once the mapper has been chosen.
///
/// On success the tenant is inserted into `req` extensions; on failure the
/// response short-circuits the chain.
#[allow(clippy::result_large_err)] // The error is the short-circuit response.
pub fn external_tenant_from_hostname<B>(
    req: &mut Request<B>,
    tenant_for_hostname: impl FnOnce(&str) -> Result<i64, TenantLookupError>,
) -> Result<(), Response<BoxBody>> {
    let host = extract_host(&request_host(req)).to_owned();
    let tenant_id = tenant_for_hostname(&host).map_err(|error| match error {
        TenantLookupError::NotFound => http_error(
            StatusCode::BAD_REQUEST,
            format!("tenant {host:?} not known"),
        ),
        TenantLookupError::Other => {
            http_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to fetch tenant")
        }
    })?;
    req.extensions_mut().insert(TenantId(tenant_id));
    Ok(())
}

/// `headerKeyTenantID` (`internal/tenant/http.go`).
pub const HEADER_TENANT_ID: &str = "x-sourcegraph-tenant-id";
/// `headerValueNoTenant`.
pub const TENANT_NONE: &str = "none";

/// `tenant.InternalHTTPMiddleware`: only the internal listener trusts
/// `X-Sourcegraph-Tenant-ID`. Empty/`none` leaves the request without a tenant;
/// a non-numeric value is a 403 written without a `Content-Type` (the baseline
/// calls `WriteHeader` then `Write`); a valid value stores the tenant.
#[allow(clippy::result_large_err)] // The error is the short-circuit response.
pub fn internal_tenant_from_header<B>(req: &mut Request<B>) -> Result<(), Response<BoxBody>> {
    let Some(value) = req
        .headers()
        .get(HEADER_TENANT_ID)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
    else {
        return Ok(());
    };
    match value.as_str() {
        "" | TENANT_NONE => Ok(()),
        other => match other.parse::<i64>() {
            Ok(tenant_id) => {
                req.extensions_mut().insert(TenantId(tenant_id));
                Ok(())
            }
            Err(_) => {
                // Go formats the error with its display name (canonical casing).
                let body =
                    "X-Sourcegraph-Tenant-ID was provided for tenant, but the value was invalid"
                        .to_owned();
                let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
                *response.status_mut() = StatusCode::FORBIDDEN;
                Err(response)
            }
        },
    }
}

/// `net/http.Error`: content type, `nosniff`, status, message + newline.
pub fn http_error(status: StatusCode, message: impl Into<String>) -> Response<BoxBody> {
    let body = format!("{}\n", message.into());
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = status;
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Request, StatusCode};

    fn request(host: &str) -> Request<()> {
        Request::builder()
            .uri("/")
            .header(header::HOST, host)
            .body(())
            .expect("request")
    }

    #[test]
    fn extract_host_only_handles_host_port() {
        assert_eq!(extract_host("example.com:3080"), "example.com");
        assert_eq!(extract_host("example.com"), "example.com");
        // Mirrors the baseline's documented IPv6 limitation.
        assert_eq!(extract_host("::1"), ":");
    }

    #[test]
    fn success_inserts_the_tenant_extension() {
        let mut req = request("example.com:3080");
        assert!(external_tenant_from_hostname(&mut req, fixed_tenant_one).is_ok());
        assert_eq!(req.extensions().get::<TenantId>(), Some(&TenantId(1)));
    }

    #[test]
    fn not_found_mapper_is_a_400() {
        let mut req = request("unknown.example:3080");
        let response =
            external_tenant_from_hostname(&mut req, |_| Err(TenantLookupError::NotFound))
                .expect_err("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
        assert!(req.extensions().get::<TenantId>().is_none());
    }

    #[test]
    fn other_mapper_errors_are_a_500() {
        let mut req = request("example.com");
        let response = external_tenant_from_hostname(&mut req, |_| Err(TenantLookupError::Other))
            .expect_err("response");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
