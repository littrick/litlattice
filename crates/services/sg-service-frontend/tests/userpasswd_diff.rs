//! M9.10c differential harness: replays the same corpus as the Go oracle
//! (`cmd/frontend/internal/auth/userpasswd/zz_m910c_oracle_test.go`) through
//! `sg_service_frontend::userpasswd`'s handlers with in-memory fakes, and
//! emits the same normalized JSON (status / body / headers / Set-Cookie
//! names / recorded store effects).
//!
//! Gated on `M910C_CORPUS`; `run_userpasswd_diff.sh` sets it and points
//! `M910C_OUT` at the output file.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{self, HeaderValue};
use http::{Method, Request, Response};
use http_body_util::{BodyExt, Full};
use sg_session::write::FixedSessionIdSource;
use sg_session::{SessionStore, SessionStoreError};
use serde_json::Value;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_service_frontend::actor_chain::ChainActor;
use sg_service_frontend::actor_middleware::Actor as ChainActorValue;
use sg_service_frontend::user_provisioning::UsernameSuffixSource;
use sg_service_frontend::userpasswd::{
    self, AccessRequestData, EmailSender, EventSink, LatestVerification, LockoutStore,
    NewUserData, PasswordHasher, SessionDeps, StoreError, UserIdentity, UserpasswdDeps,
    UserpasswdStore, VerificationCodeSource,
};

/// Field order matches Go's `resultRow`, so `json.MarshalIndent` and
/// `serde_json::to_string_pretty` produce byte-identical files.
#[derive(Debug, serde::Serialize)]
struct ResultRow {
    name: String,
    status: u16,
    body: String,
    content_type: String,
    x_content_type_options: String,
    location: String,
    set_cookies: String,
    effects: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct ActorSpec {
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    internal: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct StoreSpec {
    #[serde(default)]
    users_by_username: String,
    #[serde(default)]
    users_by_verified_email: String,
    #[serde(default)]
    is_password: String,
    #[serde(default)]
    latest_verification: String,
    #[serde(default)]
    create_user: String,
    #[serde(default)]
    grant_pending: String,
    #[serde(default)]
    set_last_verification: String,
    #[serde(default)]
    primary_email: String,
    #[serde(default)]
    primary_verified: bool,
    #[serde(default)]
    set_password: String,
    #[serde(default)]
    verify_email: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    access_request: String,
    #[serde(default)]
    is_site_admin: String,
    #[serde(default)]
    reset_url: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct LockoutSpec {
    #[serde(default)]
    locked: bool,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    unlock_email_sent: bool,
    #[serde(default)]
    verify_unlock: String,
    #[serde(default)]
    send_error: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    handler: String,
    #[serde(default)]
    method: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    site: Value,
    #[serde(default)]
    dotcom: bool,
    #[serde(default)]
    actor: ActorSpec,
    #[serde(default)]
    cookies: String,
    #[serde(default)]
    store: StoreSpec,
    #[serde(default)]
    lockout: LockoutSpec,
    #[serde(default)]
    verification_code: String,
    #[serde(default)]
    suffix: String,
    #[serde(default)]
    email_error: bool,
    #[serde(default)]
    license_expired: bool,
    #[serde(default)]
    user_site_admin: bool,
    #[serde(default)]
    user_id: i32,
}

type Effects = Arc<Mutex<Vec<String>>>;

fn effect(effects: &Effects, text: String) {
    effects.lock().expect("effects").push(text);
}

fn normalized(effects: &Effects) -> String {
    let mut items = effects.lock().expect("effects").clone();
    items.sort();
    items.join("|")
}

impl Case {
    fn user(&self) -> UserIdentity {
        UserIdentity {
            id: if self.user_id == 0 { 1 } else { self.user_id },
            username: "alice".to_owned(),
            site_admin: self.user_site_admin,
            // 2020-01-02T03:04:05Z, the oracle's fixed createdAt.
            created_at: time::OffsetDateTime::from_unix_timestamp(1_577_934_245)
                .expect("fixed timestamp"),
        }
    }

    fn suffix(&self) -> String {
        if self.suffix.is_empty() {
            "-zz".to_owned()
        } else {
            self.suffix.clone()
        }
    }
}

struct FakeStore {
    case: Case,
    effects: Effects,
}

#[async_trait]
impl UserpasswdStore for FakeStore {
    async fn user_by_username(&self, username: &str) -> Result<Option<UserIdentity>, StoreError> {
        effect(&self.effects, format!("lookup_username:{username}"));
        match self.case.store.users_by_username.as_str() {
            "user" => Ok(Some(self.case.user())),
            "error" => Err(StoreError::Other("user lookup boom".to_owned())),
            _ => Ok(None),
        }
    }

    async fn user_by_verified_email(
        &self,
        email: &str,
    ) -> Result<Option<UserIdentity>, StoreError> {
        effect(&self.effects, format!("lookup_email:{email}"));
        match self.case.store.users_by_verified_email.as_str() {
            "user" => Ok(Some(self.case.user())),
            "error" => Err(StoreError::Other("user lookup boom".to_owned())),
            _ => Ok(None),
        }
    }

    async fn is_password(&self, user_id: i32, _password: &str) -> Result<bool, StoreError> {
        effect(&self.effects, format!("is_password:{user_id}"));
        match self.case.store.is_password.as_str() {
            "true" => Ok(true),
            "error" => Err(StoreError::Other("is password boom".to_owned())),
            _ => Ok(false),
        }
    }

    async fn latest_verification_sent_email(
        &self,
        email: &str,
    ) -> Result<Option<LatestVerification>, StoreError> {
        effect(&self.effects, format!("latest_verification:{email}"));
        match self.case.store.latest_verification.as_str() {
            "cool_down" => Ok(Some(LatestVerification {
                needs_cool_down: true,
            })),
            "error" => Err(StoreError::Other("latest verification boom".to_owned())),
            _ => Ok(None),
        }
    }

    async fn create_user(&self, user: &NewUserData) -> Result<UserIdentity, StoreError> {
        effect(
            &self.effects,
            format!(
                "create_user:{}:{}:verified={}:fail_if_not_initial={}:has_password={}",
                user.email,
                user.username,
                user.email_is_verified,
                user.fail_if_not_initial_user,
                user.password_hash.is_some(),
            ),
        );
        match self.case.store.create_user.as_str() {
            "username_exists" => Err(StoreError::UsernameExists),
            "email_exists" => Err(StoreError::EmailExists),
            "presentation" => Err(StoreError::Presentation("Presentation failure.".to_owned())),
            "error" => Err(StoreError::Other("create user boom".to_owned())),
            _ => Ok(UserIdentity {
                id: 2,
                username: user.username.clone(),
                site_admin: self.case.user_site_admin,
                created_at: self.case.user().created_at,
            }),
        }
    }

    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), StoreError> {
        effect(&self.effects, format!("grant_pending:{user_id}"));
        if self.case.store.grant_pending == "error" {
            return Err(StoreError::Other("grant pending boom".to_owned()));
        }
        Ok(())
    }

    async fn set_last_verification(
        &self,
        user_id: i32,
        email: &str,
        _code: &str,
    ) -> Result<(), StoreError> {
        effect(
            &self.effects,
            format!("set_last_verification:{user_id}:{email}"),
        );
        if self.case.store.set_last_verification == "error" {
            return Err(StoreError::Other("set last verification boom".to_owned()));
        }
        Ok(())
    }

    async fn primary_email(&self, user_id: i32) -> Result<(String, bool), StoreError> {
        effect(&self.effects, format!("primary_email:{user_id}"));
        if self.case.store.primary_email == "error" {
            return Err(StoreError::Other("primary email boom".to_owned()));
        }
        Ok((
            "alice@example.com".to_owned(),
            self.case.store.primary_verified,
        ))
    }

    async fn username_by_id(&self, _user_id: i32) -> Result<Option<String>, StoreError> {
        // The Go oracle's `Users().GetByID` hook returns `c.user()`
        // unconditionally and records no effect.
        Ok(Some(self.case.user().username))
    }

    async fn set_password(
        &self,
        user_id: i32,
        code: &str,
        password: &str,
    ) -> Result<bool, StoreError> {
        effect(
            &self.effects,
            format!("set_password:{user_id}:{code}:{password}"),
        );
        match self.case.store.set_password.as_str() {
            "true" => Ok(true),
            "error" => Err(StoreError::Other("set password boom".to_owned())),
            _ => Ok(false),
        }
    }

    async fn verify_email(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
    ) -> Result<bool, StoreError> {
        effect(
            &self.effects,
            format!("verify_email:{user_id}:{email}:{code}"),
        );
        match self.case.store.verify_email.as_str() {
            "true" => Ok(true),
            "error" => Err(StoreError::Other("verify email boom".to_owned())),
            _ => Ok(false),
        }
    }

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError> {
        effect(&self.effects, format!("namespace:{name}"));
        match self.case.store.namespace.as_str() {
            "exists" => Ok(true),
            "error" => Err(StoreError::Other("namespace boom".to_owned())),
            _ => Ok(false),
        }
    }

    async fn create_access_request(
        &self,
        request: &AccessRequestData,
    ) -> Result<(), StoreError> {
        effect(
            &self.effects,
            format!(
                "access_request:{}:{}:{}",
                request.name, request.email, request.additional_info
            ),
        );
        match self.case.store.access_request.as_str() {
            "user_email_exists" => Err(StoreError::UserEmailExists),
            "access_request_email_exists" => Err(StoreError::AccessRequestEmailExists),
            "presentation" => Err(StoreError::Presentation(
                "Access request presentation failure.".to_owned(),
            )),
            "error" => Err(StoreError::Other("access request boom".to_owned())),
            _ => Ok(()),
        }
    }

    async fn is_site_admin(&self, user_id: i32) -> Result<bool, StoreError> {
        effect(&self.effects, format!("is_site_admin:{user_id}"));
        match self.case.store.is_site_admin.as_str() {
            "true" => Ok(true),
            "error" => Err(StoreError::Other("site admin boom".to_owned())),
            _ => Ok(false),
        }
    }

    async fn make_password_reset_url(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<String, StoreError> {
        effect(&self.effects, format!("reset_url:{user_id}:{email}"));
        match self.case.store.reset_url.as_str() {
            "rate_limit" => Err(StoreError::RateLimit),
            "error" => Err(StoreError::Other("reset url boom".to_owned())),
            _ => Ok("/password-reset?code=abc&email=alice%40example.com&userID=1".to_owned()),
        }
    }
}

struct FakeLockout {
    case: Case,
    effects: Effects,
}

#[async_trait]
impl LockoutStore for FakeLockout {
    async fn is_locked_out(&self, user_id: i32) -> (String, bool) {
        effect(&self.effects, format!("lockout_is_locked:{user_id}"));
        (self.case.lockout.reason.clone(), self.case.lockout.locked)
    }

    async fn increase_failed_attempt(&self, user_id: i32) {
        effect(&self.effects, format!("lockout_increase:{user_id}"));
    }

    async fn reset(&self, user_id: i32) {
        effect(&self.effects, format!("lockout_reset:{user_id}"));
    }

    async fn generate_unlock_account_url(
        &self,
        user_id: i32,
    ) -> Result<(String, String), String> {
        effect(&self.effects, format!("lockout_generate:{user_id}"));
        Ok((
            "http://example.com/unlock?token=t".to_owned(),
            "t".to_owned(),
        ))
    }

    async fn verify_unlock_account_token_and_reset(
        &self,
        token: &str,
    ) -> Result<bool, String> {
        effect(&self.effects, format!("unlock_verify:{token}"));
        match self.case.lockout.verify_unlock.as_str() {
            "true" => Ok(true),
            "error" => Err("unlock verify boom".to_owned()),
            _ => Ok(false),
        }
    }

    async fn send_unlock_account_email(&self, user_id: i32, email: &str) -> Result<(), String> {
        effect(&self.effects, format!("unlock_send:{user_id}:{email}"));
        if self.case.lockout.send_error {
            return Err("unlock send boom".to_owned());
        }
        Ok(())
    }

    async fn unlock_email_sent(&self, user_id: i32) -> bool {
        effect(&self.effects, format!("unlock_email_sent:{user_id}"));
        self.case.lockout.unlock_email_sent
    }
}

struct FakeHasher;

impl PasswordHasher for FakeHasher {
    fn hash(&self, _password: &str) -> Result<String, String> {
        Ok("$2a$10$fakefakefakefakefakefakefakefakefakefakefakefakefakefakefake".to_owned())
    }

    fn verify(&self, _hash: &str, _password: &str) -> bool {
        false
    }
}

struct FakeEmail {
    case: Case,
    effects: Effects,
}

#[async_trait]
impl EmailSender for FakeEmail {
    async fn send(&self, _source: &str, message: sg_txemail::Message) -> Result<(), String> {
        // Matches the oracle's `txemail.MockSend`:
        // `effects.add("email:%s", strings.Join(message.To, ","))`.
        effect(&self.effects, format!("email:{}", message.to.join(",")));
        if self.case.email_error {
            return Err("email boom".to_owned());
        }
        Ok(())
    }
}

struct FakeEvents {
    effects: Effects,
}

#[async_trait]
impl EventSink for FakeEvents {
    async fn security_event(
        &self,
        name: &str,
        url_path: &str,
        user_id: i32,
        anonymous_uid: &str,
    ) {
        effect(
            &self.effects,
            format!("security:{name}:{url_path}:{user_id}:{anonymous_uid}"),
        );
    }
}

struct FakeCode(String);

impl VerificationCodeSource for FakeCode {
    fn make_code(&self) -> Result<String, String> {
        Ok(self.0.clone())
    }
}

struct FakeSuffix(String);

impl UsernameSuffixSource for FakeSuffix {
    fn add_random_suffix(&self, username: &str) -> Result<String, String> {
        Ok(format!("{username}{}", self.0))
    }
}

#[derive(Default)]
struct FakeSessionStore {
    data: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl SessionStore for FakeSessionStore {
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        Ok(self.data.lock().expect("session").get(id).cloned())
    }

    async fn save(
        &self,
        id: &str,
        payload: Vec<u8>,
        _ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        self.data
            .lock()
            .expect("session")
            .insert(id.to_owned(), payload);
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
        self.data.lock().expect("session").remove(id);
        Ok(())
    }
}

fn header_value(response: &Response<BoxBody>, name: header::HeaderName) -> String {
    response
        .headers()
        .get(name)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default()
}

fn cookie_names(response: &Response<BoxBody>) -> String {
    let mut names = Vec::new();
    for value in response.headers().get_all(header::SET_COOKIE) {
        let text = String::from_utf8_lossy(value.as_bytes());
        let name = text.split('=').next().unwrap_or("").to_owned();
        names.push(name);
    }
    names.sort();
    names.join(",")
}

fn build_deps(case: &Case, effects: &Effects) -> UserpasswdDeps {
    let site = SiteConfiguration::parse(&case.site.to_string()).expect("site config");
    UserpasswdDeps {
        site,
        external_url: "https://example.com".to_owned(),
        dotcom: case.dotcom,
        store: Arc::new(FakeStore {
            case: case.clone(),
            effects: Arc::clone(effects),
        }),
        lockout: Arc::new(FakeLockout {
            case: case.clone(),
            effects: Arc::clone(effects),
        }),
        hasher: Arc::new(FakeHasher),
        email: Arc::new(FakeEmail {
            case: case.clone(),
            effects: Arc::clone(effects),
        }),
        events: Arc::new(FakeEvents {
            effects: Arc::clone(effects),
        }),
        session: SessionDeps {
            store: Arc::new(FakeSessionStore::default()),
            ids: Arc::new(FixedSessionIdSource("FIXEDID".to_owned())),
            cookie_key: b"m910c-cookie-key".to_vec(),
            secure: true,
        },
        suffix: Arc::new(FakeSuffix(case.suffix())),
        verification_codes: Arc::new(FakeCode(if case.verification_code.is_empty() {
            "CODE1".to_owned()
        } else {
            case.verification_code.clone()
        })),
        license_expired: case.license_expired,
    }
}

fn build_request(case: &Case) -> Request<Full<Bytes>> {
    let method = if case.method.is_empty() {
        "POST"
    } else {
        case.method.as_str()
    };
    let mut request = Request::builder()
        .method(Method::from_bytes(method.as_bytes()).expect("method"))
        .uri("http://example.com/.auth")
        .body(Full::new(Bytes::new()))
        .expect("request");
    if !case.cookies.is_empty() {
        request.headers_mut().insert(
            header::COOKIE,
            HeaderValue::from_str(&case.cookies).expect("cookie header"),
        );
    }
    request.extensions_mut().insert(ChainActor(ChainActorValue {
        uid: case.actor.uid,
        internal: case.actor.internal,
        ..ChainActorValue::default()
    }));
    request
}

#[tokio::test]
async fn userpasswd_diff() {
    let Ok(corpus_path) = std::env::var("M910C_CORPUS") else {
        eprintln!("M910C_CORPUS not set; skipping userpasswd differential");
        return;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let mut rows = Vec::with_capacity(cases.len());
    for case in &cases {
        let effects: Effects = Arc::new(Mutex::new(Vec::new()));
        let deps = build_deps(case, &effects);
        let request = build_request(case);
        let body = Some(case.body.as_bytes());

        let response: Response<BoxBody> = match case.handler.as_str() {
            "sign_up" => userpasswd::handle_sign_up(&deps, &request, body).await,
            "site_init" => userpasswd::handle_site_init(&deps, &request, body).await,
            "sign_in" => userpasswd::handle_sign_in(&deps, &request, body).await,
            "unlock_account" => userpasswd::handle_unlock_account(&deps, &request, body).await,
            "unlock_user_account" => {
                userpasswd::handle_unlock_user_account(&deps, &request, body).await
            }
            "reset_password_init" => {
                userpasswd::handle_reset_password_init(&deps, &request, body).await
            }
            "reset_password_code" => {
                userpasswd::handle_reset_password_code(&deps, &request, body).await
            }
            "check_username" => {
                userpasswd::handle_check_username_taken(&deps, &case.username).await
            }
            "request_access" => userpasswd::handle_request_access(&deps, &request, body).await,
            other => panic!("unknown handler {other}"),
        };

        let status = response.status().as_u16();
        let content_type = header_value(&response, header::CONTENT_TYPE);
        let x_content_type_options = header_value(&response, header::X_CONTENT_TYPE_OPTIONS);
        let location = header_value(&response, header::LOCATION);
        let cookies = cookie_names(&response);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        rows.push(ResultRow {
            name: case.name.clone(),
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
            content_type,
            x_content_type_options,
            location,
            set_cookies: cookies,
            effects: normalized(&effects),
        });
    }
    rows.sort_by(|left, right| left.name.cmp(&right.name));

    let encoded = serde_json::to_string_pretty(&rows).expect("serialize results");
    // Go's `encoding/json` escapes these three characters by default; match it
    // so the diff script can compare the files byte for byte.
    let encoded = encoded
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");

    if let Ok(out_path) = std::env::var("M910C_OUT") {
        std::fs::write(out_path, encoded).expect("write results");
    } else {
        println!("{encoded}");
    }
}
