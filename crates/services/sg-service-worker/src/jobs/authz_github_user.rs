#![forbid(unsafe_code)]

//! `internal/authz/providers/github/github.go`'s user-centric half:
//! `Provider.FetchUserPerms`, `fetchAuthenticatedUserPerms`,
//! `fetchCachedAuthenticatedUserPerms`, `getUserAffiliatedGroups` and the
//! `getAllAuthenticatedUser*` helpers.
//!
//! The baseline authenticates these requests with the *user's* OAuth token
//! (`WithAuthenticator`), decrypted from `user_external_accounts.auth_data`.
//! This module ports that machinery:
//!
//! * [`external_account_token`] mirrors `github.GetExternalAccountData`
//!   (decrypting `account_data` and `auth_data` with the user-external-account
//!   keyring, then decoding `oauth2.Token`);
//! * [`get_oauth_context`] mirrors `github.GetOAuthContext`, including the
//!   baseline quirk that an `auth.providers` GitHub entry without a `url` is a
//!   prefix of every service ID;
//! * [`AccountTokenRefresher`] mirrors
//!   `oauthtoken.GetAccountRefreshAndStoreOAuthTokenFunc` /
//!   `externalAccountTokenRefresher`: re-read the account, skip the refresh
//!   when another sync already replaced the token, otherwise POST to
//!   `/login/oauth/access_token` and persist the new token;
//! * [`OAuthBearerToken`] mirrors `auth.OAuthBearerToken` (`NeedsRefresh` with
//!   a 5-minute buffer, one refresh-and-retry on a 401).
//!
//! The database store is abstracted through [`OAuthTokenStore`] so the
//! differential harness can mock it exactly like `dbmocks.MockUserExternalAccountsStore`.
//!
//! Not ported: the `auth.FromConnection` GitHub App auther (that path is a
//! serious problem at construction, see `authz_providers`), and the
//! `WithEncryptionKey` cache wrapper (it only affects latency).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use sg_config::SiteConfiguration;
use sg_db::user_external_accounts::{self, ExternalAccount, ExternalAccountSpec};
use sg_encryption::Key;
use sqlx::postgres::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use super::authz_github::{
    can_view_org_repos_details, CachedGroup, GithubClientError, GithubUserClient, GroupsCache, Org,
};
use super::authz_github_client::GithubRestClient;
use super::perms_syncer::{ExternalUserPermissions, FetchPermsOptions, UserPermsError};

/// `auth.OAuthBearerToken.NeedsRefreshBuffer`: minutes before expiry at which
/// `FetchUserPerms` refreshes pre-emptively.
pub const NEEDS_REFRESH_BUFFER_MINUTES: i64 = 5;

/// `time.Time.IsZero`: the Go zero instant (`0001-01-01T00:00:00Z`).
fn is_go_zero_time(expiry: OffsetDateTime) -> bool {
    expiry == OffsetDateTime::from_unix_timestamp(-62_135_596_800).expect("go zero time")
}

/// `oauth2.Token`, reduced to the fields the authz provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthToken {
    pub access_token: String,
    pub token_type: String,
    pub refresh_token: String,
    /// `oauth2.Token.Expiry`; `None` is the Go zero time.
    pub expiry: Option<OffsetDateTime>,
}

impl OAuthToken {
    fn needs_refresh_with(&self, buffer_minutes: i64) -> bool {
        if self.refresh_token.is_empty() {
            return false;
        }
        let Some(expiry) = self.expiry else {
            return false;
        };
        // `auth.OAuthBearerToken.NeedsRefresh`: `!Expiry.IsZero() && ...`.
        // The Go zero time round-trips through JSON as `0001-01-01T00:00:00Z`.
        if is_go_zero_time(expiry) {
            return false;
        }
        expiry - OffsetDateTime::now_utc() <= Duration::minutes(buffer_minutes)
    }

    /// `auth.OAuthBearerToken.NeedsRefresh` with the default zero buffer (used
    /// by the refresher's "already refreshed elsewhere" comparison).
    pub fn needs_refresh(&self) -> bool {
        self.needs_refresh_with(0)
    }
}

/// `github.GetExternalAccountData`'s token half. `Ok(None)` means the account
/// has no `AuthData` (`auth_data` NULL, or the empty cipher with an empty key
/// ID, which `extsvc.NewEncryptedData` maps to nil).
pub async fn external_account_token(
    key: Option<&dyn Key>,
    account: &ExternalAccount,
) -> Result<Option<OAuthToken>, String> {
    // Decrypt `account_data` first: the baseline decodes it as a
    // `github.User` and propagates any decryption/decoding error even though
    // the authz provider does not read the value.
    if let Some(data) = encryptable(&account.account_data, &account.encryption_key_id) {
        let plain = sg_encryption::maybe_decrypt(key, data, &account.encryption_key_id)
            .await
            .map_err(|err| err.to_string())?;
        // `json.Unmarshal` into a struct pointer accepts a JSON `null`
        // (leaving the zero value), so decode through `Option`.
        serde_json::from_str::<Option<RawGithubUser>>(&plain).map_err(|err| err.to_string())?;
    }

    decrypt_stored_token(key, account).await
}

/// `encryption.DecryptJSON[oauth2.Token](acct.AuthData)`: `None` when the
/// column is NULL (or the empty cipher with an empty key ID).
pub(crate) async fn decrypt_stored_token(
    key: Option<&dyn Key>,
    account: &ExternalAccount,
) -> Result<Option<OAuthToken>, String> {
    let Some(data) = encryptable(&account.auth_data, &account.encryption_key_id) else {
        return Ok(None);
    };
    let plain = sg_encryption::maybe_decrypt(key, data, &account.encryption_key_id)
        .await
        .map_err(|err| err.to_string())?;
    let raw: RawStoredToken = serde_json::from_str::<Option<RawStoredToken>>(&plain)
        .map_err(|err| err.to_string())?
        .unwrap_or_default();
    let expiry = match raw.expiry.as_deref() {
        None => None,
        Some(expiry) => {
            let parsed = OffsetDateTime::parse(expiry, &Rfc3339)
                .map_err(|err| format!("parsing expiry: {err}"))?;
            // A stored `0001-01-01T00:00:00Z` is Go's `time.Time{}`.
            if is_go_zero_time(parsed) {
                None
            } else {
                Some(parsed)
            }
        }
    };
    Ok(Some(OAuthToken {
        access_token: raw.access_token,
        token_type: raw.token_type,
        refresh_token: raw.refresh_token,
        expiry,
    }))
}

/// `extsvc.NewEncryptedData`: nil when the SQL value is NULL or both the
/// cipher and key ID are empty.
fn encryptable<'a>(data: &'a Option<String>, key_id: &str) -> Option<&'a str> {
    let data = data.as_deref()?;
    if data.is_empty() && key_id.is_empty() {
        return None;
    }
    Some(data)
}

#[derive(Debug, Deserialize)]
struct RawGithubUser {}

#[derive(Debug, Default, Deserialize)]
struct RawStoredToken {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    expiry: Option<String>,
}

impl OAuthToken {
    /// `json.Marshal(oauthutil.Token)`: every field is unconditional in the
    /// baseline struct, including the zero `expiry`.
    fn to_stored_json(&self) -> Result<String, String> {
        let expiry = match self.expiry {
            Some(expiry) => expiry.format(&Rfc3339).map_err(|err| err.to_string())?,
            None => "0001-01-01T00:00:00Z".to_owned(),
        };
        serde_json::to_string(&serde_json::json!({
            "access_token": self.access_token,
            "token_type": self.token_type,
            "refresh_token": self.refresh_token,
            "expiry": expiry,
        }))
        .map_err(|err| err.to_string())
    }
}

/// `oauthutil.OAuthContext`, reduced to the refresh endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthContext {
    pub client_id: String,
    pub client_secret: String,
    pub token_url: String,
}

/// One `auth.providers` entry of type `github`. `url` is the raw configured
/// value (empty when unset), which `GetOAuthContext` uses for its prefix
/// match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubOAuthProvider {
    pub url: String,
    pub client_id: String,
    pub client_secret: String,
}

/// `conf.SiteConfig().AuthProviders`, filtered to GitHub entries.
pub fn github_oauth_providers(site: &SiteConfiguration) -> Vec<GithubOAuthProvider> {
    let Some(entries) = site.raw().get("auth.providers").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|entry| entry.get("type").and_then(|v| v.as_str()) == Some("github"))
        .map(|entry| GithubOAuthProvider {
            url: entry
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            client_id: entry
                .get("clientID")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            client_secret: entry
                .get("clientSecret")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
        })
        .collect()
}

/// `github.GetOAuthContext(baseURL)`.
pub fn get_oauth_context(
    providers: &[GithubOAuthProvider],
    base_url: &str,
) -> Option<OAuthContext> {
    for provider in providers {
        // `strings.TrimSuffix(p.Url, "/")` removes a single trailing slash.
        let gh_url = provider.url.strip_suffix('/').unwrap_or(&provider.url);
        if !base_url.starts_with(gh_url) {
            continue;
        }
        return Some(OAuthContext {
            client_id: provider.client_id.clone(),
            client_secret: provider.client_secret.clone(),
            token_url: format!("{gh_url}/login/oauth/access_token"),
        });
    }
    None
}

/// The `database.UserExternalAccountsStore` subset `externalAccountTokenRefresher`
/// uses.
#[async_trait]
pub trait OAuthTokenStore: Send + Sync {
    /// `store.Get(externalAccountID)`.
    async fn get(&self, id: i32) -> Result<Option<ExternalAccount>, String>;
    /// `store.Update(acct)`: persist a new `auth_data` plaintext (encrypted by
    /// the implementation), keeping the stored `account_data` bytes.
    async fn update(&self, account: &ExternalAccount, auth_data: &str) -> Result<(), String>;
}

/// Production store: Postgres + the `userExternalAccountKey` keyring.
pub struct PgOAuthTokenStore {
    pool: PgPool,
    key: Option<Arc<dyn Key>>,
}

impl PgOAuthTokenStore {
    pub fn new(pool: PgPool, key: Option<Arc<dyn Key>>) -> Self {
        Self { pool, key }
    }
}

#[async_trait]
impl OAuthTokenStore for PgOAuthTokenStore {
    async fn get(&self, id: i32) -> Result<Option<ExternalAccount>, String> {
        user_external_accounts::get_by_id(&self.pool, id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn update(&self, account: &ExternalAccount, auth_data: &str) -> Result<(), String> {
        let (encrypted_auth, auth_key_id) =
            sg_encryption::maybe_encrypt(self.key.as_deref(), auth_data)
                .await
                .map_err(|err| err.to_string())?;
        // `encryptData`: the account data was never decrypted by the refresher,
        // so `Encrypt` returns the stored cipher/key ID unchanged, and it
        // wins over the auth data's key ID (the baseline assigns it last).
        let (encrypted_account_data, key_id) =
            match (&account.account_data, account.auth_data.is_some()) {
                (Some(data), _) if !(data.is_empty() && account.encryption_key_id.is_empty()) => {
                    (data.clone(), account.encryption_key_id.clone())
                }
                _ => (String::new(), auth_key_id),
            };

        let spec = ExternalAccountSpec {
            service_type: account.service_type.clone(),
            service_id: account.service_id.clone(),
            client_id: account.client_id.clone(),
            account_id: account.account_id.clone(),
        };
        let updated = user_external_accounts::update_account_data(
            &self.pool,
            &spec,
            &encrypted_auth,
            &encrypted_account_data,
            &key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        if !updated {
            // `userExternalAccountNotFoundError{[]any{AccountSpec}}`: Go's `%v`
            // of the struct.
            return Err(format!(
                "user external account not found: [{{{} {} {} {}}}]",
                spec.service_type, spec.service_id, spec.client_id, spec.account_id
            ));
        }
        Ok(())
    }
}

/// `oauthutil.TokenRefresher`.
#[async_trait]
pub trait TokenRefresher: Send + Sync {
    async fn refresh(&self, original: &OAuthToken) -> Result<OAuthToken, String>;
}

/// `externalAccountTokenRefresher`: re-read, compare, refresh, persist.
pub struct AccountTokenRefresher {
    store: Arc<dyn OAuthTokenStore>,
    key: Option<Arc<dyn Key>>,
    external_account_id: i32,
    oauth: OAuthContext,
    http: reqwest::Client,
    /// Serialises refreshes of the same account within this process. The
    /// baseline uses `store.Transact`, whose `Get` is a plain SELECT (no row
    /// lock), so it is likewise a best-effort compare-and-refresh.
    lock: tokio::sync::Mutex<()>,
}

impl AccountTokenRefresher {
    pub fn new(
        store: Arc<dyn OAuthTokenStore>,
        key: Option<Arc<dyn Key>>,
        external_account_id: i32,
        oauth: OAuthContext,
        http: reqwest::Client,
    ) -> Self {
        Self {
            store,
            key,
            external_account_id,
            oauth,
            http,
            lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait]
impl TokenRefresher for AccountTokenRefresher {
    async fn refresh(&self, original: &OAuthToken) -> Result<OAuthToken, String> {
        let _guard = self.lock.lock().await;

        let account = self
            .store
            .get(self.external_account_id)
            .await?
            .ok_or_else(|| {
                format!(
                    "user external account not found: [{}]",
                    self.external_account_id
                )
            })?;
        // Only `auth_data` is decrypted here: the baseline's refresher never
        // touches the account data.
        let Some(fetched) = decrypt_stored_token(self.key.as_deref(), &account).await? else {
            return Err(format!(
                "no auth data found for external account id {}",
                self.external_account_id
            ));
        };

        // If the stored token differs, it was most likely refreshed in the
        // meantime.
        if fetched.access_token != original.access_token && !fetched.needs_refresh() {
            return Ok(fetched);
        }

        let refreshed = self.retrieve_token(&fetched.refresh_token).await?;
        self.store
            .update(&account, &refreshed.to_stored_json()?)
            .await
            .map_err(|err| format!("save refreshed token: {err}"))?;
        Ok(refreshed)
    }
}

impl AccountTokenRefresher {
    /// `oauthutil.RetrieveToken(doer, oauthCtx, refreshToken, AuthStyleInParams)`.
    async fn retrieve_token(&self, refresh_token: &str) -> Result<OAuthToken, String> {
        let form = [
            ("client_id", self.oauth.client_id.as_str()),
            ("client_secret", self.oauth.client_secret.as_str()),
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ];
        // Build the body before awaiting: the encoder types are not `Send`.
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        let response = self
            .http
            .post(&self.oauth.token_url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .await
            .map_err(|err| format!("refresh token: do token round trip: do request: {err}"))?;

        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or("").trim().to_owned())
            .unwrap_or_default();
        let body = response
            .text()
            .await
            .map_err(|err| format!("refresh token: do token round trip: read body: {err}"))?;

        if !status.is_success() {
            return Err(format!(
                "refresh token: do token round trip: oauth2: cannot fetch token: {} {}\nResponse: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                body
            ));
        }

        let mut token = if content_type == "application/x-www-form-urlencoded"
            || content_type == "text/plain"
        {
            let values: Vec<(String, String)> = url::form_urlencoded::parse(body.as_bytes())
                .into_owned()
                .collect();
            let get = |key: &str| {
                values
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            let error = get("error");
            if !error.is_empty() {
                return Err(format!(
                    "refresh token: do token round trip: oauth2: error in token fetch repsonse: {}\nerror_description: {}\nerror_uri: {}",
                    error,
                    get("error_description"),
                    get("error_uri")
                ));
            }
            // The form path uses `strconv.Atoi` + `expires > 0`: no int32
            // clamp, and only positive values become an expiry.
            let expires_in = get("expires_in").parse::<i64>().unwrap_or(0);
            let expiry = if expires_in > 0 {
                Some(OffsetDateTime::now_utc() + Duration::seconds(expires_in))
            } else {
                None
            };
            token_from_fields(
                get("access_token"),
                get("token_type"),
                get("refresh_token"),
                expiry,
            )
        } else {
            let raw: RawTokenResponse = serde_json::from_str(&body)
                .map_err(|err| format!("refresh token: do token round trip: {err}"))?;
            if let Some(error) = raw.error.filter(|error| !error.is_empty()) {
                return Err(format!(
                    "refresh token: do token round trip: oauth2: error in token fetch repsonse: {}\nerror_description: {}\nerror_uri: {}",
                    error,
                    raw.error_description.unwrap_or_default(),
                    raw.error_uri.unwrap_or_default()
                ));
            }
            // The JSON path decodes into `expirationTime` (int32, clamped)
            // and sets an expiry for any non-zero value, including negatives.
            let expires_in = raw.expires_in.unwrap_or(0).min(i32::MAX as i64);
            let expiry = if expires_in != 0 {
                Some(OffsetDateTime::now_utc() + Duration::seconds(expires_in))
            } else {
                None
            };
            token_from_fields(
                raw.access_token.unwrap_or_default(),
                raw.token_type.unwrap_or_default(),
                raw.refresh_token.unwrap_or_default(),
                expiry,
            )
        };

        if token.access_token.is_empty() {
            return Err(
                "refresh token: do token round trip: oauth2: server response missing access_token"
                    .to_owned(),
            );
        }
        // `RetrieveToken`: the server may omit the refresh token.
        if token.refresh_token.is_empty() {
            token.refresh_token = refresh_token.to_owned();
        }
        Ok(token)
    }
}

fn token_from_fields(
    access_token: String,
    token_type: String,
    refresh_token: String,
    expiry: Option<OffsetDateTime>,
) -> OAuthToken {
    OAuthToken {
        access_token,
        token_type,
        refresh_token,
        expiry,
    }
}

#[derive(Debug, Deserialize)]
struct RawTokenResponse {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
    #[serde(default)]
    error_uri: Option<String>,
}

/// `auth.OAuthBearerToken`: the user's token plus the refresh callback.
pub struct OAuthBearerToken {
    token: Mutex<OAuthToken>,
    refresher: Option<Arc<dyn TokenRefresher>>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl OAuthBearerToken {
    pub fn new(token: OAuthToken, refresher: Option<Arc<dyn TokenRefresher>>) -> Self {
        Self {
            token: Mutex::new(token),
            refresher,
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// The current `Authorization: Bearer` token.
    pub fn token(&self) -> OAuthToken {
        self.token.lock().expect("token lock poisoned").clone()
    }

    /// `NeedsRefresh` with `NeedsRefreshBuffer: 5`.
    pub fn needs_refresh(&self) -> bool {
        self.token
            .lock()
            .expect("token lock poisoned")
            .needs_refresh_with(NEEDS_REFRESH_BUFFER_MINUTES)
    }

    /// `Refresh`: updates the stored token in place.
    pub async fn refresh(&self) -> Result<(), String> {
        let _guard = self.refresh_lock.lock().await;
        let current = self.token();
        if current.refresh_token.is_empty() {
            return Err("no refresh token available".to_owned());
        }
        let Some(refresher) = &self.refresher else {
            return Err("refresh not implemented".to_owned());
        };
        let refreshed = refresher.refresh(&current).await?;
        *self.token.lock().expect("token lock poisoned") = refreshed;
        Ok(())
    }
}

/// `ClientAdapter.WithAuthenticator(oauthToken)`.
pub trait GithubUserClientFactory: Send + Sync {
    fn with_oauth_token(&self, token: Arc<OAuthBearerToken>) -> Arc<dyn GithubUserClient>;
}

impl GithubUserClientFactory for GithubRestClient {
    fn with_oauth_token(&self, token: Arc<OAuthBearerToken>) -> Arc<dyn GithubUserClient> {
        Arc::new(GithubRestClient::with_oauth_token(self, token))
    }
}

/// A failure inside the user-centric fetch: the already-wrapped message plus,
/// for GitHub API errors, the raw error the syncer's `errcode` classification
/// would find through the `errors.Wrap` chain.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PermsFailure {
    Client {
        err: GithubClientError,
        message: String,
    },
    Message {
        message: String,
    },
}

impl PermsFailure {
    fn client(err: GithubClientError) -> Self {
        Self::Client {
            message: err.message.clone(),
            err,
        }
    }

    /// `errors.Wrap(err, context)`.
    fn wrap(self, context: &str) -> Self {
        match self {
            Self::Client { err, message } => Self::Client {
                err,
                message: format!("{context}: {message}"),
            },
            Self::Message { message } => Self::Message {
                message: format!("{context}: {message}"),
            },
        }
    }

    /// The `errcode` classes the syncer branches on, in the syncer's order.
    fn into_user_perms_error(self, partial: Option<ExternalUserPermissions>) -> UserPermsError {
        match self {
            Self::Client { err, message } => {
                if err.unauthorized() {
                    UserPermsError::Unauthorized { message }
                } else if err.account_suspended() {
                    UserPermsError::AccountSuspended { message }
                } else if err.rate_limit_exceeded() {
                    UserPermsError::Temporary { message }
                } else {
                    UserPermsError::Failed { message, partial }
                }
            }
            Self::Message { message } => UserPermsError::Failed { message, partial },
        }
    }
}

/// `Provider.FetchUserPerms` as constructed by `ProvidersFromConfig`.
#[async_trait]
pub trait GithubUserPermsProvider: Send + Sync {
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError>;
}

pub struct GithubUserPerms {
    service_id: String,
    service_type: String,
    factory: Arc<dyn GithubUserClientFactory>,
    groups_cache: Option<Arc<dyn GroupsCache>>,
    sync_internal_repo_permissions: bool,
    oauth_providers: Vec<GithubOAuthProvider>,
    store: Arc<dyn OAuthTokenStore>,
    user_external_account_key: Option<Arc<dyn Key>>,
    http: reqwest::Client,
}

impl GithubUserPerms {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service_id: impl Into<String>,
        service_type: impl Into<String>,
        factory: Arc<dyn GithubUserClientFactory>,
        groups_cache: Option<Arc<dyn GroupsCache>>,
        sync_internal_repo_permissions: bool,
        oauth_providers: Vec<GithubOAuthProvider>,
        store: Arc<dyn OAuthTokenStore>,
        user_external_account_key: Option<Arc<dyn Key>>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            service_type: service_type.into(),
            factory,
            groups_cache,
            sync_internal_repo_permissions,
            oauth_providers,
            store,
            user_external_account_key,
            http,
        }
    }

    /// `getAllAuthenticatedUserAffiliatedRepositories`.
    async fn get_all_authenticated_user_affiliated_repositories(
        &self,
        client: &dyn GithubUserClient,
        affiliations: &[&str],
    ) -> (BTreeSet<String>, Option<PermsFailure>) {
        let mut repos = BTreeSet::new();
        let mut page = 1;
        loop {
            match client
                .list_affiliated_repositories(page, affiliations)
                .await
            {
                Ok((page_repos, has_next)) => {
                    repos.extend(page_repos.into_iter().map(|repo| repo.id));
                    if !has_next {
                        break;
                    }
                }
                Err(err) => {
                    return (
                        repos,
                        Some(PermsFailure::client(err).wrap("list affiliated repos for user")),
                    );
                }
            }
            page += 1;
        }
        (repos, None)
    }

    /// `getAllAuthenticatedUserOrgs`.
    async fn get_all_authenticated_user_orgs(
        &self,
        client: &dyn GithubUserClient,
    ) -> (Vec<Org>, Option<PermsFailure>) {
        let mut orgs = Vec::new();
        let mut page = 1;
        loop {
            match client.get_authenticated_user_orgs(page).await {
                Ok((page_orgs, has_next)) => {
                    orgs.extend(page_orgs);
                    if !has_next {
                        break;
                    }
                }
                Err(err) => {
                    return (
                        orgs,
                        Some(PermsFailure::client(err).wrap("list orgs for authenticated user")),
                    );
                }
            }
            page += 1;
        }
        (orgs, None)
    }

    /// `getAllInternalRepositoriesForOrg`.
    async fn get_all_internal_repositories_for_org(
        &self,
        client: &dyn GithubUserClient,
        org_login: &str,
    ) -> (Vec<String>, Option<PermsFailure>) {
        let mut repos = Vec::new();
        let mut page = 1;
        loop {
            match client
                .list_org_repositories(org_login, page, "internal")
                .await
            {
                Ok((page_repos, has_next)) => {
                    repos.extend(page_repos.into_iter().map(|repo| repo.id));
                    if !has_next {
                        break;
                    }
                }
                Err(err) => {
                    return (
                        repos,
                        Some(PermsFailure::client(err).wrap("list internal repos for org")),
                    );
                }
            }
            page += 1;
        }
        (repos, None)
    }

    /// `getAllAuthenticatedUserInternalRepositories`.
    async fn get_all_authenticated_user_internal_repositories(
        &self,
        client: &dyn GithubUserClient,
    ) -> (Vec<String>, Option<PermsFailure>) {
        let (orgs, failure) = self.get_all_authenticated_user_orgs(client).await;
        if let Some(failure) = failure {
            return (Vec::new(), Some(failure));
        }
        let mut repos = Vec::new();
        for org in orgs {
            let (org_repos, failure) = self
                .get_all_internal_repositories_for_org(client, &org.login)
                .await;
            repos.extend(org_repos);
            if let Some(failure) = failure {
                return (repos, Some(failure));
            }
        }
        (repos, None)
    }

    /// `getUserAffiliatedGroups`: orgs the user can view repos in, plus teams
    /// with repositories, populated from the cache where available.
    async fn get_user_affiliated_groups(
        &self,
        client: &dyn GithubUserClient,
        opts: &FetchPermsOptions,
    ) -> (Vec<CachedGroup>, Option<PermsFailure>) {
        let mut groups = Vec::new();
        let mut seen_groups: BTreeSet<String> = BTreeSet::new();
        // `syncGroup`; the baseline checks the raw team slug against a set
        // keyed by the full cache key, so duplicate teams are never skipped.
        macro_rules! sync_group {
            ($cache:expr, $org:expr, $team:expr) => {{
                let team: &str = $team;
                if !team.is_empty() && seen_groups.contains(team) {
                    continue;
                }
                let (mut group, exists) = $cache.get($org, team).await;
                if exists && opts.invalidate_caches {
                    $cache.invalidate(&mut group).await;
                }
                seen_groups.insert(group.key());
                groups.push(group);
            }};
        }
        let Some(cache) = self.groups_cache.as_deref() else {
            return (groups, None);
        };

        let mut page = 1;
        loop {
            match client
                .get_authenticated_user_orgs_details_and_membership(page)
                .await
            {
                Ok((orgs, has_next)) => {
                    for org in orgs {
                        if !can_view_org_repos_details(&org) {
                            continue;
                        }
                        let login = org
                            .details
                            .as_ref()
                            .map(|details| details.login.clone())
                            .unwrap_or_default();
                        sync_group!(cache, &login, "");
                    }
                    if !has_next {
                        break;
                    }
                }
                Err(err) => return (groups, Some(PermsFailure::client(err))),
            }
            page += 1;
        }

        let mut page = 1;
        loop {
            match client.get_authenticated_user_teams(page).await {
                Ok((teams, has_next)) => {
                    for team in teams {
                        if team.repos_count > 0 {
                            if let Some(organization) = &team.organization {
                                sync_group!(cache, &organization.login, &team.slug);
                            }
                        }
                    }
                    if !has_next {
                        break;
                    }
                }
                Err(err) => return (groups, Some(PermsFailure::client(err))),
            }
            page += 1;
        }

        (groups, None)
    }

    /// `fetchCachedAuthenticatedUserPerms`.
    async fn fetch_cached_authenticated_user_perms(
        &self,
        client: &dyn GithubUserClient,
        account_id: &str,
        opts: &FetchPermsOptions,
    ) -> (BTreeSet<String>, Option<PermsFailure>) {
        let mut user_repos = BTreeSet::new();
        let Some(cache) = self.groups_cache.as_deref() else {
            return (user_repos, None);
        };

        let (groups, failure) = self.get_user_affiliated_groups(client, opts).await;
        if let Some(failure) = failure {
            return (
                user_repos,
                Some(failure.wrap("get groups affiliated with user")),
            );
        }

        for mut group in groups {
            // If this is a partial cache, add self to the group.
            if let Some(users) = group.users.as_mut() {
                if !users.is_empty() && !users.iter().any(|user| user == account_id) {
                    users.push(account_id.to_owned());
                    if let Err(err) = cache.set(&group).await {
                        tracing::warn!(error = %err, "setting group");
                    }
                }
            }

            // A valid cached value is non-nil, even when empty.
            if let Some(repositories) = &group.repositories {
                user_repos.extend(repositories.iter().cloned());
                continue;
            }

            // Perform a full sync.
            group.repositories = Some(Vec::new());
            let is_org = group.team.is_empty();
            let mut page = 1;
            loop {
                let result = if is_org {
                    client.list_org_repositories(&group.org, page, "").await
                } else {
                    client
                        .list_team_repositories(&group.org, &group.team, page)
                        .await
                };
                match result {
                    Ok((repos, has_next)) => {
                        let group_repos = group.repositories.get_or_insert_with(Vec::new);
                        for repo in repos {
                            group_repos.push(repo.id.clone());
                            user_repos.insert(repo.id);
                        }
                        if !has_next {
                            break;
                        }
                    }
                    Err(err) if err.not_found || err.status == 403 => {
                        // Not transient: persist the partial group to cache so
                        // the scheduler does not retry unboundedly.
                        tracing::debug!(
                            error = %err.message,
                            "list repos for group: unexpected 403/404, persisting to cache"
                        );
                        break;
                    }
                    Err(err) => {
                        return (
                            user_repos,
                            Some(PermsFailure::client(err).wrap("list repos for group")),
                        );
                    }
                }
                page += 1;
            }

            if let Err(err) = cache.set(&group).await {
                tracing::warn!(error = %err, "setting group");
            }
        }

        (user_repos, None)
    }

    /// `fetchAuthenticatedUserPerms`: the private repository IDs (with partial
    /// results on failure) for an authenticated user client.
    async fn fetch_authenticated_user_perms(
        &self,
        client: &dyn GithubUserClient,
        account_id: &str,
        opts: &FetchPermsOptions,
    ) -> (ExternalUserPermissions, Option<PermsFailure>) {
        // With groups caching on, only a subset of direct affiliations is
        // synced: 'organization' repos come from the org/team groups.
        let affiliations: Vec<&str> = if self.groups_cache.is_some() {
            vec!["owner", "collaborator"]
        } else {
            Vec::new()
        };

        let mut user_repos = BTreeSet::new();
        let perms = |repos: &BTreeSet<String>| ExternalUserPermissions {
            exacts: repos.iter().cloned().collect(),
            ..ExternalUserPermissions::default()
        };

        let (repos, failure) = self
            .get_all_authenticated_user_affiliated_repositories(client, &affiliations)
            .await;
        user_repos.extend(repos);
        if let Some(failure) = failure {
            return (
                perms(&user_repos),
                Some(failure.wrap("list repos for user")),
            );
        }

        if self.groups_cache.is_none() && self.sync_internal_repo_permissions {
            let (repos, failure) = self
                .get_all_authenticated_user_internal_repositories(client)
                .await;
            user_repos.extend(repos);
            if let Some(failure) = failure {
                return (perms(&user_repos), Some(failure));
            }
        }

        if self.groups_cache.is_some() {
            let (repos, failure) = self
                .fetch_cached_authenticated_user_perms(client, account_id, opts)
                .await;
            user_repos.extend(repos);
            if let Some(failure) = failure {
                return (perms(&user_repos), Some(failure));
            }
        }

        (perms(&user_repos), None)
    }
}

#[async_trait]
impl GithubUserPermsProvider for GithubUserPerms {
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String| UserPermsError::Failed {
            message,
            partial: None,
        };

        if account.service_id != self.service_id || account.service_type != self.service_type {
            return Err(failed(format!(
                "not a code host of the account: want {:?} but have {:?}",
                account.service_id, self.service_id
            )));
        }

        let token = match external_account_token(self.user_external_account_key.as_deref(), account)
            .await
        {
            Ok(Some(token)) => token,
            Ok(None) => {
                return Err(failed(
                    "no token found in the external account data".to_owned(),
                ))
            }
            Err(err) => return Err(failed(format!("get external account data: {err}"))),
        };

        let base_url = self
            .service_id
            .strip_suffix('/')
            .unwrap_or(&self.service_id);
        let Some(oauth) = get_oauth_context(&self.oauth_providers, base_url) else {
            return Err(failed(format!(
                "no matching GitHub OAuth provider found for service {:?}",
                self.service_id
            )));
        };

        let refresher = Arc::new(AccountTokenRefresher::new(
            Arc::clone(&self.store),
            self.user_external_account_key.clone(),
            account.id,
            oauth,
            self.http.clone(),
        ));
        let bearer = Arc::new(OAuthBearerToken::new(token, Some(refresher)));
        let client = self.factory.with_oauth_token(bearer);

        let (perms, failure) = self
            .fetch_authenticated_user_perms(client.as_ref(), &account.account_id, opts)
            .await;
        match failure {
            None => Ok(Some(perms)),
            Some(failure) => Err(failure.into_user_perms_error(Some(perms))),
        }
    }
}
