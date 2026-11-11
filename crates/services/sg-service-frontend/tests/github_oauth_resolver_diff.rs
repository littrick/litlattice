//! M9.8jp differential of the GitHub OAuth user resolver against the real Go
//! baseline (`cmd/frontend/internal/auth/githuboauth/zz_m9jp_oracle_test.go`,
//! run by `run_github_oauth_resolver_diff.sh`).
//!
//! The Go oracle drives the real `sessionIssuerHelper.GetOrCreateUser` and the
//! real `auth.GetAndSaveUser` with a fake GitHub API (custom `RoundTripper`, no
//! network) and an in-memory mock DB. It records the HTTP requests (URL,
//! Authorization, Accept), the provisioning writes, the provider identity and
//! the safe error text. This harness replays the same corpus through
//! [`github_oauth::GithubOAuthResolver`] with a fake transport and an in-memory
//! [`ProvisionStore`], and writes the same JSON shape for a byte-for-byte
//! `cmp`.
//!
//! Env:
//!
//! * `SG_M9JP_HARNESS=1`
//! * `M9JP_OUT` — directory containing `corpus.json` (written by the Go oracle);
//!   one `<name>.json` per case is written under `M9JP_OUT/rust/`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use sg_service_frontend::github_oauth::{
    GithubHttpRequest, GithubHttpResponse, GithubHttpTransport, GithubOAuthResolver,
};
use sg_service_frontend::oauth_login::{go_json_escape, OAuthUserResolver, UserResolution};
use sg_service_frontend::user_provisioning::{
    CreateUserStoreError, ExternalIdentity, NewUserRow, ProvisionStore, UserProfileRow,
    UsernameSuffixSource,
};

const SUFFIX: &str = "-ubioa";
const NEXT_USER_ID: i32 = 10001;

#[derive(Deserialize)]
struct Api {
    status: u16,
    body: String,
}

#[derive(Deserialize)]
struct SeedUser {
    id: i32,
    username: String,
    #[serde(rename = "displayName", default)]
    display_name: String,
    #[serde(rename = "avatarUrl", default)]
    avatar_url: String,
    #[serde(default)]
    emails: Vec<String>,
}

#[derive(Deserialize)]
struct SeedExt {
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "userId")]
    user_id: i32,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "baseUrl")]
    base_url: String,
    #[serde(rename = "allowSignup")]
    allow_signup: bool,
    #[serde(rename = "accessToken")]
    access_token: String,
    #[serde(rename = "userApi")]
    user_api: Api,
    #[serde(rename = "emailsApi")]
    emails_api: Api,
    #[serde(rename = "seedUsers", default)]
    seed_users: Vec<SeedUser>,
    #[serde(rename = "seedExtAccounts", default)]
    seed_ext_accounts: Vec<SeedExt>,
}

/// Builds a JSON object whose keys are emitted in sorted order, matching Go's
/// `json.MarshalIndent` over `map[string]any`.
fn object(pairs: Vec<(&str, Value)>) -> Value {
    let mut map: BTreeMap<String, Value> = BTreeMap::new();
    for (key, value) in pairs {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map.into_iter().collect())
}

fn string_array(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

fn optional_string(value: Option<&str>) -> Value {
    match value {
        Some(value) => Value::String(value.to_owned()),
        None => Value::Null,
    }
}

/// Fake GitHub REST transport: serves `/user` and `/user/emails`, records each
/// request exactly like the oracle's `RoundTripper`.
struct FakeHttp {
    user: Api,
    emails: Api,
    requests: Mutex<Vec<GithubHttpRequest>>,
}

impl FakeHttp {
    fn new(user: Api, emails: Api) -> Self {
        Self {
            user,
            emails,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn snapshot(&self) -> Vec<GithubHttpRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl GithubHttpTransport for FakeHttp {
    async fn execute(&self, request: &GithubHttpRequest) -> Result<GithubHttpResponse, String> {
        self.requests.lock().unwrap().push(request.clone());
        let api = if request.url.contains("/emails") {
            &self.emails
        } else {
            &self.user
        };
        Ok(GithubHttpResponse {
            status: api.status,
            body: api.body.clone(),
        })
    }
}

struct FixedSuffix;

impl UsernameSuffixSource for FixedSuffix {
    fn add_random_suffix(&self, username: &str) -> Result<String, String> {
        // The oracle replaces `userpasswd.MockAddRandomSuffix` with an
        // unconditional `username + "-ubioa"`.
        Ok(format!("{username}{SUFFIX}"))
    }
}

#[derive(Clone)]
struct UserRow {
    id: i32,
    username: String,
    display_name: String,
    avatar_url: String,
    emails: Vec<String>,
}

#[derive(Clone)]
struct ExtRow {
    service_type: String,
    service_id: String,
    client_id: String,
    account_id: String,
    user_id: i32,
}

#[derive(Default)]
struct StoreState {
    users: Vec<UserRow>,
    ext: Vec<ExtRow>,
    next_id: i32,
    writes_users: Vec<Value>,
    writes_emails: Vec<Value>,
    writes_updates: Vec<Value>,
    writes_ext: Vec<Value>,
}

struct InMemoryStore {
    state: Mutex<StoreState>,
}

impl InMemoryStore {
    fn new(case: &Case) -> Self {
        let users = case
            .seed_users
            .iter()
            .map(|user| UserRow {
                id: user.id,
                username: user.username.clone(),
                display_name: user.display_name.clone(),
                avatar_url: user.avatar_url.clone(),
                emails: user.emails.clone(),
            })
            .collect();
        let ext = case
            .seed_ext_accounts
            .iter()
            .map(|acct| ExtRow {
                service_type: acct.service_type.clone(),
                service_id: acct.service_id.clone(),
                client_id: acct.client_id.clone(),
                account_id: acct.account_id.clone(),
                user_id: acct.user_id,
            })
            .collect();
        Self {
            state: Mutex::new(StoreState {
                users,
                ext,
                next_id: NEXT_USER_ID,
                ..StoreState::default()
            }),
        }
    }

    fn ext_write(identity: &ExternalIdentity<'_>, user_id: i32, op: &str) -> Value {
        object(vec![
            ("accountId", Value::String(identity.account_id.to_owned())),
            ("clientId", Value::String(identity.client_id.to_owned())),
            ("op", Value::String(op.to_owned())),
            ("serviceId", Value::String(identity.service_id.to_owned())),
            (
                "serviceType",
                Value::String(identity.service_type.to_owned()),
            ),
            ("userId", Value::from(user_id)),
        ])
    }

    fn snapshot(&self) -> (Vec<Value>, Vec<Value>, Vec<Value>, Vec<Value>) {
        let state = self.state.lock().unwrap();
        (
            state.writes_users.clone(),
            state.writes_emails.clone(),
            state.writes_updates.clone(),
            state.writes_ext.clone(),
        )
    }
}

fn matches_ext(row: &ExtRow, identity: &ExternalIdentity<'_>) -> bool {
    row.service_type == identity.service_type
        && row.service_id == identity.service_id
        && row.client_id == identity.client_id
        && row.account_id == identity.account_id
}

#[async_trait]
impl ProvisionStore for InMemoryStore {
    async fn update_external_account(
        &self,
        identity: &ExternalIdentity<'_>,
    ) -> Result<Option<i32>, String> {
        let mut state = self.state.lock().unwrap();
        let found = state
            .ext
            .iter()
            .find(|row| matches_ext(row, identity))
            .map(|row| row.user_id);
        if let Some(user_id) = found {
            state
                .writes_ext
                .push(Self::ext_write(identity, user_id, "touch"));
        }
        Ok(found)
    }

    async fn find_user_by_verified_email(&self, email: &str) -> Result<Option<i32>, String> {
        let state = self.state.lock().unwrap();
        Ok(state
            .users
            .iter()
            .find(|user| user.emails.iter().any(|candidate| candidate == email))
            .map(|user| user.id))
    }

    async fn find_user_by_username(&self, username: &str) -> Result<Option<i32>, String> {
        let state = self.state.lock().unwrap();
        Ok(state
            .users
            .iter()
            .find(|user| user.username == username)
            .map(|user| user.id))
    }

    async fn get_user_profile(&self, id: i32) -> Result<Option<UserProfileRow>, String> {
        let state = self.state.lock().unwrap();
        Ok(state
            .users
            .iter()
            .find(|user| user.id == id)
            .map(|user| UserProfileRow {
                id: user.id,
                display_name: user.display_name.clone(),
                avatar_url: user.avatar_url.clone(),
            }))
    }

    async fn update_user_profile(
        &self,
        id: i32,
        display_name: Option<&str>,
        avatar_url: Option<&str>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if let Some(user) = state.users.iter_mut().find(|user| user.id == id) {
            if let Some(value) = display_name {
                user.display_name = value.to_owned();
            }
            if let Some(value) = avatar_url {
                user.avatar_url = value.to_owned();
            }
        }
        state.writes_updates.push(object(vec![
            ("avatarUrl", optional_string(avatar_url)),
            ("displayName", optional_string(display_name)),
            ("id", Value::from(id)),
        ]));
        Ok(())
    }

    async fn create_with_external_account(
        &self,
        new_user: &NewUserRow<'_>,
        identity: &ExternalIdentity<'_>,
    ) -> Result<i32, CreateUserStoreError> {
        let mut state = self.state.lock().unwrap();
        if state
            .users
            .iter()
            .any(|user| user.username == new_user.username)
        {
            return Err(CreateUserStoreError::UsernameExists);
        }
        if state
            .users
            .iter()
            .any(|user| user.emails.iter().any(|email| email == new_user.email))
        {
            return Err(CreateUserStoreError::EmailExists);
        }

        let user_id = state.next_id;
        state.next_id += 1;
        state.users.push(UserRow {
            id: user_id,
            username: new_user.username.to_owned(),
            display_name: new_user.display_name.to_owned(),
            avatar_url: new_user.avatar_url.to_owned(),
            emails: vec![new_user.email.to_owned()],
        });
        state.writes_users.push(object(vec![
            ("avatarUrl", Value::String(new_user.avatar_url.to_owned())),
            (
                "displayName",
                Value::String(new_user.display_name.to_owned()),
            ),
            ("id", Value::from(user_id)),
            ("username", Value::String(new_user.username.to_owned())),
        ]));
        state.writes_emails.push(object(vec![
            ("email", Value::String(new_user.email.to_owned())),
            ("primary", Value::Bool(true)),
            ("userId", Value::from(user_id)),
            ("verified", Value::Bool(new_user.email_is_verified)),
        ]));
        state.ext.push(ExtRow {
            service_type: identity.service_type.to_owned(),
            service_id: identity.service_id.to_owned(),
            client_id: identity.client_id.to_owned(),
            account_id: identity.account_id.to_owned(),
            user_id,
        });
        state
            .writes_ext
            .push(Self::ext_write(identity, user_id, "create"));
        Ok(user_id)
    }

    async fn upsert_external_account(
        &self,
        user_id: i32,
        identity: &ExternalIdentity<'_>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.ext.iter().find(|row| matches_ext(row, identity)) {
            if existing.user_id != user_id {
                return Err(format!(
                    "unable to change association of external account from user {} to user {} (delete the external account and then try again)",
                    existing.user_id, user_id
                ));
            }
        } else {
            state.ext.push(ExtRow {
                service_type: identity.service_type.to_owned(),
                service_id: identity.service_id.to_owned(),
                client_id: identity.client_id.to_owned(),
                account_id: identity.account_id.to_owned(),
                user_id,
            });
        }
        state
            .writes_ext
            .push(Self::ext_write(identity, user_id, "upsert"));
        Ok(())
    }
}

fn request_value(request: &GithubHttpRequest) -> Value {
    object(vec![
        ("accept", string_array(&request.accept)),
        (
            "authorization",
            Value::String(request.authorization.clone()),
        ),
        ("method", Value::String(request.method.clone())),
        ("url", Value::String(request.url.clone())),
    ])
}

#[test]
fn github_oauth_resolver_harness() {
    if std::env::var("SG_M9JP_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let out = std::env::var("M9JP_OUT").expect("M9JP_OUT");
    let out_dir = Path::new(&out);
    let rust_dir = out_dir.join("rust");
    std::fs::create_dir_all(&rust_dir).expect("rust dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(out_dir.join("corpus.json")).expect("corpus"))
            .expect("corpus json");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for case in &cases {
        let http = FakeHttp::new(
            Api {
                status: case.user_api.status,
                body: case.user_api.body.clone(),
            },
            Api {
                status: case.emails_api.status,
                body: case.emails_api.body.clone(),
            },
        );
        let store = InMemoryStore::new(case);
        let suffix = FixedSuffix;
        let resolver = GithubOAuthResolver::new(
            case.service_id.clone(),
            case.client_id.clone(),
            case.allow_signup,
            &case.base_url,
            &http,
            &store,
            &suffix,
            false,
        );
        let resolution = runtime.block_on(resolver.resolve(&case.access_token));
        let (new_user, user_id, safe_err_msg) = match resolution {
            UserResolution::Created { user_id, new_user } => (new_user, user_id, String::new()),
            UserResolution::Failed { safe_message } => (false, 0, safe_message),
        };

        let requests: Vec<Value> = http.snapshot().iter().map(request_value).collect();
        let (users, user_emails, user_updates, user_external_accounts) = store.snapshot();
        let record = object(vec![
            ("clientId", Value::String(case.client_id.clone())),
            (
                "failureEventName",
                Value::String(resolver.failure_event_name().to_owned()),
            ),
            ("name", Value::String(case.name.clone())),
            ("newUser", Value::Bool(new_user)),
            ("requests", Value::Array(requests)),
            ("safeErrMsg", Value::String(safe_err_msg)),
            ("serviceId", Value::String(resolver.service_id().to_owned())),
            (
                "successEventName",
                Value::String(resolver.success_event_name().to_owned()),
            ),
            ("userId", Value::from(user_id)),
            (
                "writes",
                object(vec![
                    ("userEmails", Value::Array(user_emails)),
                    ("userExternalAccounts", Value::Array(user_external_accounts)),
                    ("userUpdates", Value::Array(user_updates)),
                    ("users", Value::Array(users)),
                ]),
            ),
        ]);
        let mut json = serde_json::to_string_pretty(&record).expect("encode");
        json = go_json_escape(&json);
        let path = rust_dir.join(format!("{}.json", case.name));
        std::fs::write(&path, json.as_bytes()).expect("write");
        println!("RUST {} newUser={} userId={}", case.name, new_user, user_id);
    }
}
