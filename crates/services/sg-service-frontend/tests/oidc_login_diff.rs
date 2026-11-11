//! M9.8jr differential of the OpenID Connect provider and resolver against the
//! real Go baseline (`cmd/frontend/internal/auth/openidconnect/zz_m9jr_oracle_test.go`,
//! run by `run_oidc_login_diff.sh`).
//!
//! The Go oracle drives `GetProviderAndClient`, `providerConfigID`,
//! `Verifier.Verify`, `UserInfo` and `getOrCreateUser` against a fake OP
//! transport with fixed RSA/EC keys, and writes a `corpus.json` containing the
//! discovery/JWKS/userinfo responses and already-signed id_tokens. This
//! harness feeds that corpus through [`oidc_login`] with a fake
//! [`OidcHttpClient`] and a recording [`Provisioner`], then writes the same
//! JSON shape for a byte-for-byte `cmp`.
//!
//! Env:
//!
//! * `SG_OIDC_LOGIN_HARNESS=1`
//! * `OIDC_LOGIN_CORPUS` — `corpus.json` written by the Go oracle
//! * `OIDC_LOGIN_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use sg_service_frontend::oauth_login::{OAuthToken, OAuthUserResolver};
use sg_service_frontend::oidc_login::{
    provider_config_id, ExternalIdentity, OidcHttpClient, OidcHttpResponse, OidcLoginResolver,
    OpenIdConnectAuthProvider, ProvisionError, ProvisionOutcome, Provisioner, FAILURE_EVENT_NAME,
    SUCCESS_EVENT_NAME,
};

#[derive(Deserialize, Clone)]
struct Case {
    name: String,
    config: Value,
    discovery: HttpJson,
    jwks: HttpJson,
    userinfo: HttpJson,
    id_token: String,
    access_token: String,
    token_type: String,
    expected_nonce: String,
    provision: ProvisionJson,
    now_unix: i64,
}

#[derive(Deserialize, Clone)]
struct HttpJson {
    status: u16,
    body: String,
}

#[derive(Deserialize, Clone)]
struct ProvisionJson {
    outcome: String,
    user_id: i32,
    safe_message: String,
}

struct FakeHttp {
    routes: HashMap<String, (u16, Vec<u8>)>,
}

#[async_trait]
impl OidcHttpClient for FakeHttp {
    async fn get(&self, url: &str, _bearer: Option<&str>) -> Result<OidcHttpResponse, String> {
        match self.routes.get(url) {
            Some((status, body)) => Ok(OidcHttpResponse {
                status: *status,
                body: body.clone(),
            }),
            None => Err(format!("unexpected request to {url}")),
        }
    }
}

#[derive(Default)]
struct FakeProvisioner {
    outcome: String,
    user_id: i32,
    safe_message: String,
    seen: Mutex<Option<IdentityOut>>,
}

#[async_trait]
impl Provisioner for FakeProvisioner {
    async fn get_or_create_user(
        &self,
        identity: &ExternalIdentity<'_>,
    ) -> Result<ProvisionOutcome, ProvisionError> {
        *self.seen.lock().unwrap() = Some(IdentityOut {
            service_type: identity.service_type.to_owned(),
            service_id: identity.service_id.to_owned(),
            account_id: identity.account_id.to_owned(),
            login: identity.login.to_owned(),
            email: identity.email.to_owned(),
            email_verified: identity.email_verified,
            display_name: identity.display_name.unwrap_or_default().to_owned(),
        });
        match self.outcome.as_str() {
            "error" => Err(ProvisionError {
                safe_message: self.safe_message.clone(),
                message: "provision failed".to_owned(),
            }),
            "existing" => Ok(ProvisionOutcome::Existing {
                user_id: self.user_id,
            }),
            _ => Ok(ProvisionOutcome::Created {
                user_id: self.user_id,
            }),
        }
    }
}

#[derive(Serialize, Clone, PartialEq, Eq)]
struct ClaimsOut {
    name: String,
    given_name: String,
    family_name: String,
    preferred_username: String,
    picture: String,
    email_verified: Option<bool>,
}

#[derive(Serialize)]
struct DiscoveryOut {
    ok: bool,
    error: String,
    safe_error: String,
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
    end_session_endpoint: String,
    revocation_endpoint: String,
    algorithms: Vec<String>,
}

#[derive(Serialize)]
struct VerifyOut {
    ok: bool,
    error: String,
    issuer: String,
    subject: String,
    audience: Vec<String>,
    expiry_unix: i64,
    nonce: String,
    claims: ClaimsOut,
}

#[derive(Serialize)]
struct NonceOut {
    checked: bool,
    ok: bool,
    error: String,
}

#[derive(Serialize)]
struct UserinfoOut {
    ok: bool,
    error: String,
    email: String,
    email_verified: bool,
    subject: String,
    profile: String,
}

#[derive(Serialize, Clone, PartialEq, Eq)]
struct IdentityOut {
    service_type: String,
    service_id: String,
    account_id: String,
    login: String,
    email: String,
    email_verified: bool,
    display_name: String,
}

#[derive(Serialize)]
struct ResolverOut {
    outcome: String,
    user_id: i32,
    new_user: bool,
    safe_message: String,
    service_id: String,
    success_event: String,
    failure_event: String,
    identity: Option<IdentityOut>,
}

#[derive(Serialize)]
struct Output {
    name: String,
    config_id: String,
    discovery: DiscoveryOut,
    verify: VerifyOut,
    nonce: NonceOut,
    userinfo: UserinfoOut,
    resolver: ResolverOut,
}

fn empty_claims() -> ClaimsOut {
    ClaimsOut {
        name: String::new(),
        given_name: String::new(),
        family_name: String::new(),
        preferred_username: String::new(),
        picture: String::new(),
        email_verified: None,
    }
}

async fn run_case(case: &Case) -> Output {
    let config: OpenIdConnectAuthProvider = serde_json::from_value(case.config.clone()).unwrap();
    let mut routes = HashMap::new();
    routes.insert(
        format!(
            "{}/.well-known/openid-configuration",
            config.issuer.trim_end_matches('/')
        ),
        (
            case.discovery.status,
            case.discovery.body.clone().into_bytes(),
        ),
    );
    if let Ok(document) = serde_json::from_str::<Value>(&case.discovery.body) {
        if let Some(jwks_uri) = document.get("jwks_uri").and_then(Value::as_str) {
            routes.insert(
                jwks_uri.to_owned(),
                (case.jwks.status, case.jwks.body.clone().into_bytes()),
            );
        }
        if let Some(userinfo_endpoint) = document.get("userinfo_endpoint").and_then(Value::as_str) {
            routes.insert(
                userinfo_endpoint.to_owned(),
                (
                    case.userinfo.status,
                    case.userinfo.body.clone().into_bytes(),
                ),
            );
        }
    }
    let http = Arc::new(FakeHttp { routes });
    let provisioner = Arc::new(FakeProvisioner {
        outcome: case.provision.outcome.clone(),
        user_id: case.provision.user_id,
        safe_message: case.provision.safe_message.clone(),
        seen: Mutex::new(None),
    });

    let resolver = OidcLoginResolver::new(config.clone(), http, provisioner.clone(), case.now_unix);
    let resolver = if case.expected_nonce.is_empty() {
        resolver
    } else {
        resolver.with_expected_nonce(case.expected_nonce.clone())
    };

    let auth = resolver
        .authorize(&case.access_token, &case.token_type, &case.id_token)
        .await;
    let token = OAuthToken::from_json_value(json!({
        "access_token": case.access_token,
        "token_type": case.token_type,
        "id_token": case.id_token,
    }))
    .unwrap();
    let resolution = resolver.get_or_create_user(&token).await;

    let discovery = match &auth.provider {
        Some(provider) => DiscoveryOut {
            ok: true,
            error: String::new(),
            safe_error: String::new(),
            authorization_endpoint: provider.document.authorization_endpoint.clone(),
            token_endpoint: provider.document.token_endpoint.clone(),
            userinfo_endpoint: provider.document.userinfo_endpoint.clone(),
            end_session_endpoint: provider.document.end_session_endpoint.clone(),
            revocation_endpoint: provider.document.revocation_endpoint.clone(),
            algorithms: provider.algorithms(),
        },
        None => DiscoveryOut {
            ok: false,
            error: auth.discovery_error.clone(),
            safe_error: auth.safe_message.clone().unwrap_or_default(),
            authorization_endpoint: String::new(),
            token_endpoint: String::new(),
            userinfo_endpoint: String::new(),
            end_session_endpoint: String::new(),
            revocation_endpoint: String::new(),
            algorithms: Vec::new(),
        },
    };

    let verify = match &auth.id_token {
        Some(id_token) => VerifyOut {
            ok: true,
            error: String::new(),
            issuer: id_token.issuer.clone(),
            subject: id_token.subject.clone(),
            audience: id_token.audience.clone(),
            expiry_unix: id_token.expiry_unix,
            nonce: id_token.nonce.clone(),
            claims: ClaimsOut {
                name: id_token.claims.name.clone(),
                given_name: id_token.claims.given_name.clone(),
                family_name: id_token.claims.family_name.clone(),
                preferred_username: id_token.claims.preferred_username.clone(),
                picture: id_token.claims.picture.clone(),
                email_verified: id_token.claims.email_verified,
            },
        },
        None => VerifyOut {
            ok: false,
            error: auth.verify_error.clone(),
            issuer: String::new(),
            subject: String::new(),
            audience: Vec::new(),
            expiry_unix: 0,
            nonce: String::new(),
            claims: empty_claims(),
        },
    };

    let nonce = NonceOut {
        checked: auth.nonce_checked,
        ok: auth.nonce_ok,
        error: if auth.nonce_checked && !auth.nonce_ok {
            sg_service_frontend::oidc_login::NONCE_SAFE_ERROR.to_owned()
        } else {
            String::new()
        },
    };

    let userinfo = match &auth.userinfo {
        Some(userinfo) => UserinfoOut {
            ok: true,
            error: String::new(),
            email: userinfo.email.clone(),
            email_verified: userinfo.email_verified,
            subject: userinfo.subject.clone(),
            profile: userinfo.profile.clone(),
        },
        None => UserinfoOut {
            ok: false,
            error: auth.userinfo_error.clone(),
            email: String::new(),
            email_verified: false,
            subject: String::new(),
            profile: String::new(),
        },
    };

    let resolver_out = match resolution {
        sg_service_frontend::oauth_login::UserResolution::Created { user_id, new_user } => {
            ResolverOut {
                outcome: if new_user { "created" } else { "existing" }.to_owned(),
                user_id,
                new_user,
                safe_message: String::new(),
                service_id: config.issuer.clone(),
                success_event: SUCCESS_EVENT_NAME.to_owned(),
                failure_event: FAILURE_EVENT_NAME.to_owned(),
                identity: provisioner.seen.lock().unwrap().clone(),
            }
        }
        // OIDC never returns a status-bearing failure (that variant exists for
        // `gitlaboauth.CallbackHandler`); keep it out of the normalized output.
        sg_service_frontend::oauth_login::UserResolution::Failed { safe_message }
        | sg_service_frontend::oauth_login::UserResolution::FailedWithStatus {
            safe_message, ..
        } => ResolverOut {
            outcome: "failed".to_owned(),
            user_id: 0,
            new_user: false,
            safe_message,
            service_id: config.issuer.clone(),
            success_event: SUCCESS_EVENT_NAME.to_owned(),
            failure_event: FAILURE_EVENT_NAME.to_owned(),
            identity: None,
        },
    };

    Output {
        name: case.name.clone(),
        config_id: provider_config_id(&config),
        discovery,
        verify,
        nonce,
        userinfo,
        resolver: resolver_out,
    }
}

#[tokio::test]
async fn oidc_login_diff() {
    if std::env::var("SG_OIDC_LOGIN_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("OIDC_LOGIN_CORPUS").expect("OIDC_LOGIN_CORPUS");
    let out_dir = std::env::var("OIDC_LOGIN_OUT").expect("OIDC_LOGIN_OUT");
    let corpus: Vec<Case> =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("read corpus"))
            .expect("parse corpus");
    std::fs::create_dir_all(Path::new(&out_dir)).expect("create out dir");
    for case in &corpus {
        let output = run_case(case).await;
        let json = serde_json::to_string_pretty(&output).expect("encode");
        std::fs::write(
            Path::new(&out_dir).join(format!("{}.json", case.name)),
            json,
        )
        .expect("write case");
        println!("RUST {} outcome={}", case.name, output.resolver.outcome);
    }
    println!("RUST cases={}", corpus.len());
}
