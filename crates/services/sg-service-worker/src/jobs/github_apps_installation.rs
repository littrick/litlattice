#![forbid(unsafe_code)]

//! `cmd/worker/internal/githubapps` + `internal/github_apps` for the
//! `github-apps-installation-validation-job`.
//!
//! Every `github_apps` row whose GitHub App ID is non-zero is reconciled
//! against `GET <api>/app/installations?page=N`, authenticated with an RS256
//! JWT signed by the app's private key:
//!
//! - the remote installations are upserted (new and existing alike, so the
//!   stored account metadata is refreshed);
//! - installations present in the database but absent remotely are removed;
//! - a `404` from GitHub means the app itself was deleted, so every stored
//!   installation is removed and the error is still reported;
//! - any other fetch error leaves the stored installations untouched.
//!
//! Per-app errors are accumulated and flattened into the baseline's
//! `errors.MultiError` shape (`N errors occurred:` for more than one). The job
//! runs every 24 hours as `github_apps.installation_backfill`.
//!
//! Ported here: the job wrapper, `NewGitHubInstallationWorker.Handle`,
//! `store.SyncInstallations`/`List`/`GetInstallations`/`Install`/
//! `BulkRemoveInstallations` (SQL in `sg-db::github_apps`), the RSA JWT
//! authenticator, `extsvc/github.APIRoot`, the Link-header pagination and
//! `APIError` status handling. Not ported: log lines, RED metrics, the
//! installation-access-token authenticator and the remaining store methods
//! (`Create`/`Update`/`GetBy*`), none of which this job calls.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::RsaPrivateKey;
use serde::Deserialize;
use sg_db::github_apps::{self, GitHubApp, GitHubAppInstallation};
use sg_encryption::{maybe_decrypt, Key};
use sg_keyring::Keyring;
use sha2::Sha256;
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `githubAppsInstallationJob.Description`.
pub const JOB_DESCRIPTION: &str = "Job to validate and backfill github app installations";

/// `goroutine.WithName`.
pub const ROUTINE_NAME: &str = "github_apps.installation_backfill";
/// `goroutine.WithDescription`.
pub const ROUTINE_DESCRIPTION: &str =
    "backfills github apps installation ids and removes deleted github app installations";
/// `goroutine.WithInterval(24 * time.Hour)`.
pub const INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

const ACCEPT_HEADER: &str =
    "application/vnd.github.jean-grey-preview+json,application/vnd.github.mercy-preview+json";
const ACCEPT_APP_HEADER: &str = "application/vnd.github.machine-man-preview+json";

/// `gogithub.Installation`, restricted to the fields the store reads. `id` is
/// a pointer in go-github (`nil` entries are skipped); the account is a
/// pointer whose getters are nil-safe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteInstallation {
    pub id: Option<i64>,
    pub html_url: String,
    pub account: Option<RemoteAccount>,
}

/// `gogithub.User`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteAccount {
    pub login: String,
    pub avatar_url: String,
    pub html_url: String,
    pub account_type: String,
}

/// `extsvc/github.APIError` reduced to what the job observes: the HTTP status
/// (nil for transport/other errors) and the formatted message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    pub status: Option<u16>,
    pub message: String,
}

impl ClientError {
    pub fn api(status: u16, message: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            message: message.into(),
        }
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self {
            status: None,
            message: message.into(),
        }
    }

    /// `errors.As(err, &e) && e.Code == http.StatusNotFound`.
    pub fn is_not_found(&self) -> bool {
        self.status == Some(404)
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// `ghtypes.GitHubAppClient`.
#[async_trait]
pub trait GitHubAppClient: Send + Sync {
    /// `GetAppInstallations(ctx, page)`: the page's installations and whether
    /// the `Link` header advertises `rel="next"`.
    async fn get_app_installations(
        &self,
        page: i32,
    ) -> Result<(Vec<RemoteInstallation>, bool), ClientError>;
}

/// `newGithubClient`: builds one client per app. Injected so the differential
/// can supply `MockGitHubClient`-style clients.
pub trait GitHubAppClientFactory: Send + Sync {
    fn create(&self, app: &GitHubApp) -> Result<Arc<dyn GitHubAppClient>, String>;
}

/// The production factory: parse the private key, map the base URL to the API
/// root and build the V3 client.
#[derive(Debug, Default)]
pub struct RealGitHubAppClientFactory {
    http: reqwest::Client,
}

impl RealGitHubAppClientFactory {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }
}

impl GitHubAppClientFactory for RealGitHubAppClientFactory {
    fn create(&self, app: &GitHubApp) -> Result<Arc<dyn GitHubAppClient>, String> {
        // Baseline order: `auth.NewGitHubAppAuthenticator` parses the key
        // before the base URL is parsed.
        let signing_key = parse_private_key(&app.private_key)?;
        let base_url = reqwest::Url::parse(&app.base_url).map_err(|err| err.to_string())?;
        Ok(Arc::new(V3Client {
            http: self.http.clone(),
            api_url: api_root(&base_url),
            app_id: app.app_id,
            signing_key,
        }))
    }
}

/// `auth.NewGitHubAppAuthenticator`: `jwt.ParseRSAPrivateKeyFromPEM` accepts
/// both PKCS#1 (`RSA PRIVATE KEY`) and PKCS#8 (`PRIVATE KEY`) PEM blocks.
fn parse_private_key(pem: &str) -> Result<SigningKey<Sha256>, String> {
    let key = RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .map_err(|err| format!("parse private key: {err}"))?;
    Ok(SigningKey::new(key))
}

/// `github.APIRoot`: GitHub.com's API lives on `api.github.com`; a GitHub
/// Enterprise root gains `/api/v3`, and one with a path prefix gains `api`.
pub fn api_root(base_url: &reqwest::Url) -> reqwest::Url {
    let host = base_url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host == "github.com" || host == "www.github.com" {
        return reqwest::Url::parse("https://api.github.com/").expect("static API URL");
    }
    if base_url.path().is_empty() || base_url.path() == "/" {
        return base_url
            .join("/api/v3")
            .expect("absolute path joins against any base");
    }
    base_url
        .join("api")
        .expect("relative path joins against any base")
}

/// `V3Client.GetAppInstallations` request URL: `path.Join(apiURL.Path,
/// "app/installations")` resolved against the API root, with `?page=N`.
fn installations_url(api_url: &reqwest::Url, page: i32) -> reqwest::Url {
    let mut url = api_url.clone();
    url.set_path(&join_path(api_url.path(), "app/installations"));
    url.set_query(Some(&format!("page={page}")));
    url
}

/// `path.Join` for the two path components the client combines.
fn join_path(base: &str, rest: &str) -> String {
    let joined = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        rest.trim_start_matches('/')
    );
    if joined.starts_with('/') {
        joined
    } else {
        format!("/{joined}")
    }
}

/// `V3Client` for the `rest` resource.
struct V3Client {
    http: reqwest::Client,
    api_url: reqwest::Url,
    app_id: i32,
    signing_key: SigningKey<Sha256>,
}

impl V3Client {
    /// `GitHubAppAuthenticator.generateJWT`: `iss` is the app ID, `iat` is one
    /// minute in the past (truncated to the second) and `exp` is ten minutes
    /// after `iat`. GitHub rejects fractional timestamps, hence the truncation.
    fn jwt(&self) -> Result<String, String> {
        let iat = OffsetDateTime::now_utc().unix_timestamp() - 60;
        let exp = iat + 10 * 60;
        let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = encoder.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "iss": self.app_id.to_string(),
            "iat": iat,
            "exp": exp,
        });
        let claims = encoder.encode(serde_json::to_vec(&claims).map_err(|err| err.to_string())?);
        let signing_input = format!("{header}.{claims}");
        let signature = self.signing_key.sign(signing_input.as_bytes());
        Ok(format!(
            "{signing_input}.{}",
            encoder.encode(signature.to_vec())
        ))
    }
}

/// `gogithub.Installation` JSON, restricted to the fields the store reads.
#[derive(Debug, Deserialize, Default)]
struct RawInstallation {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    html_url: String,
    #[serde(default)]
    account: Option<RawAccount>,
}

#[derive(Debug, Deserialize, Default)]
struct RawAccount {
    #[serde(default)]
    login: String,
    #[serde(default)]
    avatar_url: String,
    #[serde(default)]
    html_url: String,
    #[serde(default, rename = "type")]
    account_type: String,
}

impl From<RawInstallation> for RemoteInstallation {
    fn from(raw: RawInstallation) -> Self {
        Self {
            id: raw.id,
            html_url: raw.html_url,
            account: raw.account.map(|account| RemoteAccount {
                login: account.login,
                avatar_url: account.avatar_url,
                html_url: account.html_url,
                account_type: account.account_type,
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawApiError {
    #[serde(default)]
    message: String,
}

#[async_trait]
impl GitHubAppClient for V3Client {
    async fn get_app_installations(
        &self,
        page: i32,
    ) -> Result<(Vec<RemoteInstallation>, bool), ClientError> {
        let url = installations_url(&self.api_url, page);
        let jwt = self.jwt().map_err(ClientError::other)?;

        // `doRequest`: two Accept values, JSON content type and
        // `Cache-Control: max-age=0`.
        let response = self
            .http
            .get(url.clone())
            .header("Accept", ACCEPT_HEADER)
            .header("Accept", ACCEPT_APP_HEADER)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("Cache-Control", "max-age=0")
            .bearer_auth(jwt)
            .send()
            .await
            .map_err(|err| ClientError::other(format!("request failed: {err}")))?;

        let status = response.status();
        let code = status.as_u16();
        let has_next_page = response
            .headers()
            .get("Link")
            .and_then(|value| value.to_str().ok())
            .map(|link| link.contains("rel=\"next\""))
            .unwrap_or(false);

        if !(200..400).contains(&code) {
            // `APIError` body decoding; the baseline reads at most 8 KiB.
            let bytes = response.bytes().await.unwrap_or_default();
            let bytes = &bytes[..bytes.len().min(8 * 1024)];
            let body = String::from_utf8_lossy(bytes).into_owned();
            let message = match serde_json::from_str::<RawApiError>(&body) {
                Ok(error) => error.message,
                Err(err) => {
                    format!("failed to decode error response from GitHub API: {err}: {body:?}")
                }
            };
            return Err(ClientError::api(
                code,
                format!("request to {url} returned status {code}: {message}"),
            ));
        }

        // A 304 carries no body (the baseline returns early); the request is
        // unconditional, so this is defensive only.
        if code == 304 {
            return Ok((Vec::new(), has_next_page));
        }

        let body = response
            .bytes()
            .await
            .map_err(|err| ClientError::other(format!("request failed: {err}")))?;
        let installations: Vec<RawInstallation> = serde_json::from_slice(&body)
            .map_err(|err| ClientError::other(format!("decode response: {err}")))?;
        Ok((
            installations.into_iter().map(Into::into).collect(),
            has_next_page,
        ))
    }
}

/// `gitHubAppsStore.list` + `decrypt`: fetch every app and decrypt the
/// client secret and private key. A failure here is the baseline's `List`
/// error, which `Handle` wraps with `Fetching GitHub Apps`.
async fn list_decrypted(
    frontend: &PgPool,
    key: Option<&dyn Key>,
) -> Result<Vec<GitHubApp>, String> {
    let mut apps = github_apps::list(frontend).await?;
    for app in &mut apps {
        app.client_secret = maybe_decrypt(key, &app.client_secret, &app.encryption_key_id)
            .await
            .map_err(|err| err.to_string())?;
        app.private_key = maybe_decrypt(key, &app.private_key, &app.encryption_key_id)
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(apps)
}

/// `Store.SyncInstallations`: paginate the GitHub API, upsert every remote
/// installation, then delete the database rows the API no longer reports.
/// A 404 from GitHub removes every stored installation for the app. Errors
/// are returned rather than logged.
pub async fn sync_installations(
    frontend: &PgPool,
    app: &GitHubApp,
    client: &dyn GitHubAppClient,
) -> Vec<String> {
    let mut errors = Vec::new();

    let db_installations = match github_apps::get_installations(frontend, app.id).await {
        Ok(installations) => installations,
        Err(err) => return vec![err],
    };

    // `getAllAppInstallations`: page until the Link header stops advertising
    // `rel="next"`.
    let mut remote_installations = Vec::new();
    let mut page = 1;
    loop {
        match client.get_app_installations(page).await {
            Ok((installations, has_next_page)) => {
                remote_installations.extend(installations);
                if !has_next_page {
                    break;
                }
                page += 1;
            }
            Err(err) => {
                errors.push(err.to_string());

                // A 404 means the GitHub App itself was deleted; drop every
                // stored installation. Other errors preserve them.
                if err.is_not_found() {
                    let installation_ids: Vec<i32> = db_installations
                        .iter()
                        .map(|install| install.installation_id)
                        .collect();
                    if !installation_ids.is_empty() {
                        if let Err(delete_err) = github_apps::bulk_remove_installations(
                            frontend,
                            app.id,
                            &installation_ids,
                        )
                        .await
                        {
                            errors.push(delete_err);
                            return errors;
                        }
                    }
                }

                return errors;
            }
        }
    }

    let mut missing: std::collections::BTreeSet<i32> = db_installations
        .iter()
        .map(|install| install.installation_id)
        .collect();

    let mut to_be_added = Vec::new();
    for install in remote_installations {
        let Some(id) = install.id else {
            continue;
        };
        let id = i32::try_from(id).unwrap_or(i32::MAX);
        let account = install.account.unwrap_or_default();
        to_be_added.push(GitHubAppInstallation {
            id: 0,
            app_id: app.id,
            installation_id: id,
            url: Some(install.html_url),
            account_login: Some(account.login),
            account_avatar_url: Some(account.avatar_url),
            account_url: Some(account.html_url),
            account_type: Some(account.account_type),
        });
        // Remote installations are upserted regardless of presence; removing
        // them from the set is what marks the row as still existing.
        missing.remove(&id);
    }

    for install in to_be_added {
        if let Err(err) = github_apps::install(frontend, &install).await {
            errors.push(err);
        }
    }

    if !missing.is_empty() {
        let installation_ids: Vec<i32> = missing.into_iter().collect();
        if let Err(err) =
            github_apps::bulk_remove_installations(frontend, app.id, &installation_ids).await
        {
            errors.push(err);
            return errors;
        }
    }

    errors
}

/// `githubAppInstallationWorker.Handle`: list the apps, then create a client
/// and sync the installations for every app with a non-zero GitHub App ID.
pub async fn handle(
    frontend: &PgPool,
    keyring: &Keyring,
    factory: &dyn GitHubAppClientFactory,
) -> Result<(), String> {
    let key = keyring.github_app_key();
    let apps = list_decrypted(frontend, key.as_deref())
        .await
        .map_err(|err| format!("Fetching GitHub Apps: {err}"))?;

    let mut errors = Vec::new();
    for app in apps {
        if app.app_id == 0 {
            continue;
        }

        let client = match factory.create(&app) {
            Ok(client) => client,
            Err(err) => {
                errors.push(err);
                continue;
            }
        };

        errors.extend(sync_installations(frontend, &app, client.as_ref()).await);
    }

    match combine_errors(&errors) {
        Some(message) => Err(message),
        None => Ok(()),
    }
}

/// The periodic handler around [`handle`].
pub struct InstallationHandler {
    frontend: PgPool,
    keyring: Arc<Keyring>,
    factory: Arc<dyn GitHubAppClientFactory>,
}

impl InstallationHandler {
    pub fn new(
        frontend: PgPool,
        keyring: Arc<Keyring>,
        factory: Arc<dyn GitHubAppClientFactory>,
    ) -> Self {
        Self {
            frontend,
            keyring,
            factory,
        }
    }
}

#[async_trait]
impl Handler for InstallationHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        handle(&self.frontend, &self.keyring, self.factory.as_ref())
            .await
            .map_err(HandlerError::new)
    }
}

/// `cmd/worker/internal/githubapps.NewGitHubApsInstallationJob`.
#[derive(Debug, Default)]
pub struct GitHubAppsInstallationValidation;

impl GitHubAppsInstallationValidation {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for GitHubAppsInstallationValidation {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let routine = PeriodicRoutine::new(Arc::new(InstallationHandler::new(
            deps.frontend.clone(),
            deps.keyring.clone(),
            Arc::new(RealGitHubAppClientFactory::new()),
        )))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(INTERVAL);

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 1024-bit key generated for these tests only (PKCS#8).
    const PKCS8_TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAL0sXOF/UzpmdZmk
I790mym5h+AcP8+BlLdIGh0QkliknzEzQk0EgxXs9I8ADh4rsZTvN8usheNzt2xp
4jaRbolT06e5xbs0OhyEhm508F2ce4HyM++oOcZkpvguovnKZkYB8aFvwstw9Yy+
Z26rXjfNLf9reMqjU1VrFlO1NpxXAgMBAAECgYAaSByLU5YrieqSsuQoiDm6XUVM
VIBlAtq3b/P4z8GVQSHAwx9V9eeEV0xOxbIwbN+aEnYM/OYJCJ2WGUvNSv0xJ+Wb
n3kZd3omUaAEhFPIyBB5b5jvJF5iPvB8bcogzm/x8Ck30XIPC9VPrGrnDbu+C50P
b9+SJGFO1JwuzX3kCQJBAN95Vqy/gjnfW8qEgPA7ODmHBU2ONPp8zV1Wyg5lOUMM
WK1eNZd5i/8+Hl1RPQrLQub6wQIVivSjfYyrlxPoZZUCQQDYtPo5n+/NhQR+VcRY
pAUkKfNDqddu6UuKikIjF+sxjAUdJ207ZmQG1gHxHGTzL5JBvof3UbQkV2lC2THU
F6c7AkEAtzahqusvdwb6GGTd2RaUGAXsgRMPuDVNoTKgYYD3m1Kp+Sj9wvwxnMdu
2Y7a/th6NyhpyK2kyMCpFAUxzW6AqQJBAMraJfz5BL2KTVmBKWmcJ5/hkPB+tMYC
pg7KOvyksClSG5tdw7c9slZfz7OGU8z8c9kM5DOTjKAUHtlPfIA/n+8CQQCkpUYx
IPRkkXJYEgRUNQivjHsEJFsQyfnAgNX/8tkdrDSYpfTU+DRv05HnXoLyPqedPPCP
wyUqYOy8qh2y33uk
-----END PRIVATE KEY-----";

    /// The same key in PKCS#1 form (`RSA PRIVATE KEY`).
    const PKCS1_TEST_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQC9LFzhf1M6ZnWZpCO/dJspuYfgHD/PgZS3SBodEJJYpJ8xM0JN
BIMV7PSPAA4eK7GU7zfLrIXjc7dsaeI2kW6JU9OnucW7NDochIZudPBdnHuB8jPv
qDnGZKb4LqL5ymZGAfGhb8LLcPWMvmduq143zS3/a3jKo1NVaxZTtTacVwIDAQAB
AoGAGkgci1OWK4nqkrLkKIg5ul1FTFSAZQLat2/z+M/BlUEhwMMfVfXnhFdMTsWy
MGzfmhJ2DPzmCQidlhlLzUr9MSflm595GXd6JlGgBIRTyMgQeW+Y7yReYj7wfG3K
IM5v8fApN9FyDwvVT6xq5w27vgudD2/fkiRhTtScLs195AkCQQDfeVasv4I531vK
hIDwOzg5hwVNjjT6fM1dVsoOZTlDDFitXjWXeYv/Ph5dUT0Ky0Lm+sECFYr0o32M
q5cT6GWVAkEA2LT6OZ/vzYUEflXEWKQFJCnzQ6nXbulLiopCIxfrMYwFHSdtO2Zk
BtYB8Rxk8y+SQb6H91G0JFdpQtkx1BenOwJBALc2oarrL3cG+hhk3dkWlBgF7IET
D7g1TaEyoGGA95tSqfko/cL8MZzHbtmO2v7YejcoacitpMjAqRQFMc1ugKkCQQDK
2iX8+QS9ik1ZgSlpnCef4ZDwfrTGAqYOyjr8pLApUhubXcO3PbJWX8+zhlPM/HPZ
DOQzk4ygFB7ZT3yAP5/vAkEApKVGMSD0ZJFyWBIEVDUIr4x7BCRbEMn5wIDV//LZ
Haw0mKX01Pg0b9OR516C8j6nnTzwj8MlKmDsvKodst97pA==
-----END RSA PRIVATE KEY-----";

    fn url(raw: &str) -> reqwest::Url {
        reqwest::Url::parse(raw).expect("test URL")
    }

    #[test]
    fn api_root_maps_github_dot_com_and_enterprise() {
        assert_eq!(
            api_root(&url("https://github.com")).as_str(),
            "https://api.github.com/"
        );
        assert_eq!(
            api_root(&url("https://www.github.com")).as_str(),
            "https://api.github.com/"
        );
        assert_eq!(
            api_root(&url("https://ghe.example.com")).as_str(),
            "https://ghe.example.com/api/v3"
        );
        assert_eq!(
            api_root(&url("https://ghe.example.com/")).as_str(),
            "https://ghe.example.com/api/v3"
        );
        assert_eq!(
            api_root(&url("https://ghe.example.com/subpath")).as_str(),
            "https://ghe.example.com/api"
        );
        assert_eq!(
            api_root(&url("https://ghe.example.com/subpath/")).as_str(),
            "https://ghe.example.com/subpath/api"
        );
    }

    #[test]
    fn installations_url_joins_the_api_path() {
        assert_eq!(
            installations_url(&url("https://api.github.com/"), 3).as_str(),
            "https://api.github.com/app/installations?page=3"
        );
        assert_eq!(
            installations_url(&url("https://ghe.example.com/api/v3"), 1).as_str(),
            "https://ghe.example.com/api/v3/app/installations?page=1"
        );
    }

    #[test]
    fn parses_both_pem_encodings_and_signs_verifiable_jwts() {
        let signing_key = parse_private_key(PKCS8_TEST_KEY).expect("PKCS#8 key");
        parse_private_key(PKCS1_TEST_KEY).expect("PKCS#1 key");

        let client = V3Client {
            http: reqwest::Client::new(),
            api_url: url("https://api.github.com/"),
            app_id: 7001,
            signing_key,
        };
        let token = client.jwt().expect("jwt");
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header: serde_json::Value =
            serde_json::from_slice(&encoder.decode(parts[0]).expect("header")).expect("json");
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");

        let claims: serde_json::Value =
            serde_json::from_slice(&encoder.decode(parts[1]).expect("claims")).expect("json");
        assert_eq!(claims["iss"], "7001");
        let iat = claims["iat"].as_i64().expect("iat");
        let exp = claims["exp"].as_i64().expect("exp");
        assert_eq!(exp - iat, 600);
        assert!((OffsetDateTime::now_utc().unix_timestamp() - iat).abs() <= 61);
    }

    #[test]
    fn non_pem_keys_are_rejected_like_go() {
        let err = parse_private_key("not a key").expect_err("must fail");
        assert!(err.starts_with("parse private key: "));
    }

    #[test]
    fn only_404_is_the_deleted_app_signal() {
        assert!(ClientError::api(404, "gone").is_not_found());
        assert!(!ClientError::api(500, "boom").is_not_found());
        assert!(!ClientError::other("network").is_not_found());
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = GitHubAppsInstallationValidation::new();
        assert_eq!(
            job.description(),
            "Job to validate and backfill github app installations"
        );
        assert!(job.configs().is_empty());
        assert_eq!(ROUTINE_NAME, "github_apps.installation_backfill");
        assert_eq!(INTERVAL, Duration::from_secs(86_400));
    }
}
