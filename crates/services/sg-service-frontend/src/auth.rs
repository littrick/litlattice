#![forbid(unsafe_code)]

//! Port of `internal/auth/forbid_all.go`: the external auth gate that rejects
//! every request while the site configuration has no authentication provider.
//!
//! The baseline reads `conf.Get().AuthProviders` (the `auth.providers` array)
//! on every request. This port reads the startup [`SiteConfiguration`]
//! snapshot, so a configuration change requires a restart until the
//! configuration service is ported (docs/overviews/m9_8ec §1.4).

use bytes::Bytes;
use http::{header, Response, StatusCode};
use http_body_util::Full;
use sg_config::SiteConfiguration;
use tonic::body::BoxBody;

/// The baseline `const msg`.
pub const FORBID_ALL_MESSAGE: &str =
    "Access to Sourcegraph is forbidden because no authentication provider is set in site configuration.";

/// `len(conf.Get().AuthProviders)`: the `auth.providers` array length.
///
/// A missing key, `null` or a non-array value all yield 0. The baseline would
/// reject a non-array value during configuration parsing, so the difference
/// is only observable for configurations the Go server never starts with.
pub fn provider_count(site: &SiteConfiguration) -> usize {
    site.raw()
        .get("auth.providers")
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

/// `ForbidAllRequestsMiddleware`'s predicate.
pub fn forbidden(site: &SiteConfiguration) -> bool {
    provider_count(site) == 0
}

/// `http.Error(w, msg, http.StatusForbidden)`.
pub fn forbidden_response() -> Response<BoxBody> {
    let body = format!("{FORBID_ALL_MESSAGE}\n");
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = StatusCode::FORBIDDEN;
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

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    #[test]
    fn provider_count_matches_the_baseline_array_length() {
        assert_eq!(provider_count(&site("{}")), 0);
        assert_eq!(provider_count(&site(r#"{"auth.providers":[]}"#)), 0);
        assert_eq!(
            provider_count(&site(r#"{"auth.providers":[{"type":"builtin"}]}"#)),
            1
        );
        assert_eq!(
            provider_count(&site(r#"{"auth.providers":[null,{"type":"builtin"}]}"#)),
            2
        );
        // A non-array value is rejected by the baseline's configuration
        // schema; the port treats it as no providers (documented deviation).
        assert_eq!(provider_count(&site(r#"{"auth.providers":{}}"#)), 0);
    }

    #[test]
    fn forbidden_only_without_providers() {
        assert!(forbidden(&site("{}")));
        assert!(forbidden(&site(r#"{"auth.providers":[]}"#)));
        assert!(!forbidden(&site(r#"{"auth.providers":[{}]}"#)));
    }

    #[test]
    fn forbidden_response_is_http_error() {
        let response = forbidden_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
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
    }
}
