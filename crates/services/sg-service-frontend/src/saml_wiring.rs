#![forbid(unsafe_code)]

//! Production wiring of the SAML auth provider into the external listener
//! (M9.8jt's [`crate::saml_auth`] seam over the actor chain).
//!
//! Baseline shape (`cmd/frontend/internal/auth/saml/middleware.go:48-78`,
//! registered in `auth/init.go:50` between OpenID Connect and the OAuth
//! providers):
//!
//! 1. App requests whose path starts with `/.auth/saml/` are handled by the
//!    SAML endpoints (`metadata`, `login`, `acs`, `logout`) before the actor
//!    is consulted.
//! 2. Otherwise an authenticated actor proceeds untouched.
//! 3. An unauthenticated App request is redirected to the IdP when the single
//!    sign-in provider is SAML, the instance is not auth-public and the
//!    sign-out cookie is absent.
//!
//! The Rust external chain computes the actor inside
//! `run_external_chain`, so the endpoint branch runs before the chain and the
//! auto-redirect branch runs after it with the resolved actor. The observable
//! order matches the baseline: endpoints never consult the actor, and the
//! auto-redirect happens before `RequireAuthMiddleware`'s sign-in redirect.
//!
//! Dependencies (session store, license reader, provisioning) are the same
//! production seams the OAuth wiring uses; no new table, column or Redis key
//! is introduced (see the M9.8jt overview, §4.4).

use std::sync::Arc;

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_session::{SessionIdSource, SessionStore, COOKIE_NAME, SIGN_OUT_COOKIE};

use crate::actor_chain::LicenseReader;
use crate::authz::LicenseState;
use crate::oauth_login::parse_query;
use crate::oauth_routes::{cookie_value, query_has};
use crate::saml_auth::{
    saml_middleware, SamlDeps, SamlHttpRequest, SamlHttpResponse,
    SamlProviderSet, SamlProvisioner, SamlUserStore, SignInProviderInfo,
};
use crate::user_provisioning::UsernameSuffixSource;

/// `authPrefix` (`saml/middleware.go:23`).
const AUTH_PREFIX: &str = "/.auth/saml";

const CONTENT_TYPE_TEXT: &str = "text/plain; charset=utf-8";

/// Everything the SAML middleware needs for one configuration generation.
///
/// The sign-in provider lists are precomputed for both `SignInProviders`
/// variants (`skip sourcegraphOperator` is a per-request decision driven by
/// the query string, exactly like [`crate::oauth_routes::OAuthRouteConfig`]).
pub struct SamlRouteDeps {
    pub providers: SamlProviderSet,
    pub sign_in_without_soap: Vec<SignInProviderInfo>,
    pub sign_in_with_soap: Vec<SignInProviderInfo>,
    /// `conf.AuthPublic()` (dotcom mode).
    pub auth_public: bool,
    pub external_url: String,
    pub store: Arc<dyn SessionStore>,
    pub ids: Arc<dyn SessionIdSource>,
    /// `SRC_SESSION_COOKIE_KEY` (empty by default, like the baseline).
    pub cookie_key: Vec<u8>,
    pub secure: bool,
    pub configured_expiry: Option<String>,
    pub license: Arc<dyn LicenseReader>,
    pub provisioner: Arc<dyn SamlProvisioner>,
    pub users: Arc<dyn SamlUserStore>,
    pub suffix: Arc<dyn UsernameSuffixSource>,
    pub sourcegraph_dotcom: bool,
}

/// Builds [`SamlRouteDeps`] for one site configuration generation
/// (`conf.Cached` in the baseline: provider edits take effect without a
/// restart).
pub type SamlRouteFactory = Arc<dyn Fn(&SiteConfiguration) -> Arc<SamlRouteDeps> + Send + Sync>;

/// Resolves a request against the SAML middleware.
///
/// `allow_auto_redirect` is `false` for the pre-chain call (the actor is not
/// known yet, so only the `/.auth/saml/` endpoints apply) and `true` for the
/// post-chain call, where `authenticated` is the chain's actor state.
///
/// The POST body is consumed only for `/.auth/saml/` endpoints: the baseline
/// calls `r.ParseForm()` there, and the request never reaches the app router.
pub async fn saml_route_response<B>(
    deps: &SamlRouteDeps,
    req: &mut Request<B>,
    authenticated: bool,
    allow_auto_redirect: bool,
) -> Option<Response<BoxBody>>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
{
    let path = req.uri().path().to_owned();
    let is_endpoint = path.starts_with(&format!("{AUTH_PREFIX}/"));
    if !allow_auto_redirect && !is_endpoint {
        return None;
    }
    if allow_auto_redirect && !is_endpoint && authenticated {
        // `saml_middleware` short-circuits here too; checking first avoids the
        // license read below.
        return None;
    }

    let form = if is_endpoint && req.method() == http::Method::POST {
        parse_form_body(req).await
    } else {
        Vec::new()
    };

    let query = req.uri().query().map(parse_query).unwrap_or_default();
    let query_string = req.uri().query().unwrap_or("").to_owned();
    let url_string = if query_string.is_empty() {
        path.clone()
    } else {
        format!("{path}?{query_string}")
    };
    let referer = req
        .headers()
        .get(header::REFERER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let session_cookie = cookie_value(req, COOKIE_NAME);
    let has_sign_out_cookie = cookie_value(req, SIGN_OUT_COOKIE).is_some();

    // `SignInProviders(!r.URL.Query().Has("sourcegraph-operator"))`.
    let skip_soap = !query_has(req, "sourcegraph-operator");
    let sign_in_providers = if skip_soap {
        &deps.sign_in_without_soap
    } else {
        &deps.sign_in_with_soap
    };

    // The license only gates user provisioning (`session.go`), so the read is
    // limited to the endpoints that can create a user.
    let license_expired = if is_endpoint {
        matches!(
            deps.license.product_license().await,
            LicenseState::Expired | LicenseState::Error
        )
    } else {
        false
    };

    let request = SamlHttpRequest {
        method: req.method().as_str().to_owned(),
        path,
        url_string,
        query,
        form,
        referer,
        session_cookie,
        has_sign_out_cookie,
        authenticated,
        is_api_request: false,
    };
    let saml_deps = SamlDeps {
        providers: &deps.providers,
        sign_in_providers,
        auth_public: deps.auth_public,
        external_url: &deps.external_url,
        store: deps.store.as_ref(),
        ids: deps.ids.as_ref(),
        cookie_key: &deps.cookie_key,
        secure: deps.secure,
        configured_expiry: deps.configured_expiry.as_deref(),
        now: OffsetDateTime::now_utc(),
        provisioner: deps.provisioner.as_ref(),
        users: deps.users.as_ref(),
        suffix: deps.suffix.as_ref(),
        sourcegraph_dotcom: deps.sourcegraph_dotcom,
        license_expired,
    };
    let response = saml_middleware(&saml_deps, &request).await?;
    Some(into_http_response(response))
}

/// `net/http.Request.ParseForm` for `application/x-www-form-urlencoded`
/// bodies. A read error leaves the form empty (the baseline ignores the
/// `ParseForm` error in these handlers too).
async fn parse_form_body<B>(req: &mut Request<B>) -> Vec<(String, String)>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
{
    let is_form = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
        .unwrap_or(false);
    if !is_form {
        return Vec::new();
    }
    match req.body_mut().collect().await {
        Ok(collected) => parse_query(String::from_utf8_lossy(&collected.to_bytes()).trim_start()),
        Err(_) => Vec::new(),
    }
}

/// [`SamlHttpResponse`] → hyper response. `http.Error` (all error paths in the
/// baseline) sets `X-Content-Type-Options: nosniff`; `http.Redirect` writes the
/// HTML body for GET requests (already in the seam's body).
fn into_http_response(response: SamlHttpResponse) -> Response<BoxBody> {
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder().status(status);
    if !response.location.is_empty() {
        builder = builder.header(header::LOCATION, response.location);
    }
    if !response.content_type.is_empty() {
        let content_type = http::HeaderValue::from_str(&response.content_type)
            .unwrap_or_else(|_| http::HeaderValue::from_static(""));
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    if response.content_type == CONTENT_TYPE_TEXT {
        builder = builder.header("x-content-type-options", "nosniff");
    }
    for cookie in response.set_cookies {
        builder = builder.header(header::SET_COOKIE, cookie.to_header());
    }
    builder
        .body(tonic::body::boxed(Full::new(Bytes::from(response.body))))
        .unwrap_or_else(|_| {
            let mut fallback = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
            *fallback.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            fallback
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;

    use crate::saml_auth::{
        SamlHttpClient, SamlMetadataResponse, SamlNewUser, SamlUser,
    };
    use crate::user_provisioning::{OsUsernameSuffix, ProvisionError, ProvisionOutcome};

    struct NoStore;

    #[async_trait]
    impl SessionStore for NoStore {
        async fn load(&self, _id: &str) -> Result<Option<Vec<u8>>, sg_session::SessionStoreError> {
            Ok(None)
        }
    }

    struct NoLicense;

    #[async_trait]
    impl LicenseReader for NoLicense {
        async fn product_license(&self) -> LicenseState {
            LicenseState::Valid
        }
    }

    struct NoProvisioner;

    #[async_trait]
    impl SamlProvisioner for NoProvisioner {
        async fn get_or_create_user(
            &self,
            _user: &SamlNewUser<'_>,
        ) -> Result<ProvisionOutcome, ProvisionError> {
            panic!("the wiring tests never provision users");
        }
    }

    struct NoUsers;

    #[async_trait]
    impl SamlUserStore for NoUsers {
        async fn get_by_id(&self, _id: i32) -> Result<Option<SamlUser>, String> {
            Ok(None)
        }
    }

    struct NoHttp;

    #[async_trait]
    impl SamlHttpClient for NoHttp {
        async fn fetch(&self, _url: &str) -> Result<SamlMetadataResponse, String> {
            Err("the wiring tests never fetch IdP metadata".to_owned())
        }
    }

    fn deps(site: &str) -> SamlRouteDeps {
        let unified = sg_config::Unified::parse(&sg_config::RawUnified {
            site: site.to_owned(),
            ..sg_config::RawUnified::default()
        })
        .expect("site configuration");
        let site = &unified.site;
        let deps = SamlRouteDeps {
            providers: SamlProviderSet::from_site(site, Arc::new(NoHttp)),
            sign_in_without_soap: crate::saml_auth::sign_in_providers_from_site(site, true),
            sign_in_with_soap: crate::saml_auth::sign_in_providers_from_site(site, false),
            auth_public: false,
            external_url: "https://sourcegraph.example.com".to_owned(),
            store: Arc::new(NoStore),
            ids: Arc::new(sg_session::OsSessionIdSource),
            cookie_key: Vec::new(),
            secure: true,
            configured_expiry: None,
            license: Arc::new(NoLicense),
            provisioner: Arc::new(NoProvisioner),
            users: Arc::new(NoUsers),
            suffix: Arc::new(OsUsernameSuffix),
            sourcegraph_dotcom: false,
        };
        deps
    }

    fn get(uri: &str) -> Request<Full<Bytes>> {
        Request::get(uri)
            .body(Full::new(Bytes::new()))
            .expect("request")
    }

    async fn body_text(response: Response<BoxBody>) -> (StatusCode, String) {
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn endpoints_are_served_without_providers() {
        let deps = deps("{}");
        let mut req = get("/.auth/saml/metadata");
        let response = saml_route_response(&deps, &mut req, false, false)
            .await
            .expect("the endpoint is handled");
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        // `http.Error` sets nosniff.
        assert_eq!(
            response.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        let (status, body) = body_text(response).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, "Misconfigured SAML auth provider\n");
    }

    #[tokio::test]
    async fn endpoints_ignore_the_actor() {
        let deps = deps("{}");
        let mut req = get("/.auth/saml/metadata");
        let response = saml_route_response(&deps, &mut req, true, false)
            .await
            .expect("endpoints run before the actor is consulted");
        assert_eq!(body_text(response).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn acs_consumes_the_form_body() {
        let deps = deps("{}");
        let mut req = Request::post("/.auth/saml/acs")
            .header(
                header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(Full::new(Bytes::from("RelayState=x&SAMLResponse=y")))
            .expect("request");
        let response = saml_route_response(&deps, &mut req, false, false)
            .await
            .expect("the ACS endpoint is handled");
        assert_eq!(body_text(response).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn non_endpoint_requests_are_left_to_the_chain() {
        let deps = deps("{}");
        let mut req = get("/");
        assert!(saml_route_response(&deps, &mut req, false, false)
            .await
            .is_none());
        // Unauthenticated, but the single sign-in provider is `builtin`, not
        // SAML: no redirect.
        assert!(saml_route_response(&deps, &mut req, false, true)
            .await
            .is_none());
        // Authenticated requests short-circuit before the license read.
        assert!(saml_route_response(&deps, &mut req, true, true)
            .await
            .is_none());
    }
}
