#![forbid(unsafe_code)]

//! `internal/authz/providers/bitbucketserver`: the Bitbucket Server / Data
//! Center authz provider.
//!
//! Two provider flavours exist in the baseline:
//!
//! * [`BitbucketServerProvider`] (`provider.go`): Sourcegraph usernames are
//!   assumed to equal Bitbucket Server usernames, so `FetchAccount` looks the
//!   user up by name. Permissions are fetched by impersonating the user
//!   through OAuth1 `Sudo`, which this port does not implement.
//! * [`BitbucketServerOAuth2Provider`] (`oauthprovider.go`): the external
//!   account's stored OAuth2 token is used to list the private repositories
//!   the user can read. The baseline wraps it in an `auth.OAuthBearerToken`
//!   with a refresh function, but `(*Client).do` only calls `Authenticate`,
//!   so this path never refreshes (and never retries a 401); the token is
//!   used as stored, including an expired one.
//!
//! Plugin mode (`plugin.permissions == "enabled"`) asks the Sourcegraph
//! Bitbucket Server plugin for a roaring bitmap
//! (`rest/sourcegraph-admin/1.0/permissions/repositories?permission=read`)
//! instead of walking `rest/api/1.0/repos?visibility=private`.
//!
//! OAuth1 is not ported: it needs RSA-SHA1 request signing with a random
//! nonce, which cannot be reproduced deterministically, so such clients are
//! rejected at construction and reported as a serious problem by the provider
//! registry (deny-by-default).

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::user_external_accounts::ExternalAccount;
use sg_encryption::Key;

use super::authz_github_user::{decrypt_stored_token, OAuthContext};
use super::bitbucket_server_client::{
    BitbucketServerClient, BitbucketServerConnection, PageToken, User, UserFilter,
};
use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    FetchedExternalAccount, UserPermsError,
};

/// `extsvc.TypeBitbucketServer`.
pub const TYPE_BITBUCKETSERVER: &str = "bitbucketServer";
/// Default page size (`NewProvider`/`NewOAuthProvider`).
pub const DEFAULT_PAGE_SIZE: i64 = 1000;
/// `errNoResults`.
pub const ERR_NO_RESULTS: &str = "no results returned by the Bitbucket Server API";

/// One `auth.providers` entry of type `bitbucketserver`, reduced to the fields
/// `GetOAuthContext` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketServerOAuthProvider {
    pub url: String,
    pub client_id: String,
    pub client_secret: String,
}

/// `conf.SiteConfig().AuthProviders` entries of type `bitbucketserver`.
pub fn bitbucketserver_oauth_providers(
    site: &SiteConfiguration,
) -> Vec<BitbucketServerOAuthProvider> {
    site.raw()
        .get("auth.providers")
        .and_then(serde_json::Value::as_array)
        .map(|providers| {
            providers
                .iter()
                .filter(|provider| {
                    provider.get("type").and_then(serde_json::Value::as_str)
                        == Some("bitbucketserver")
                })
                .map(|provider| BitbucketServerOAuthProvider {
                    url: string_field(provider, "url"),
                    client_id: string_field(provider, "clientID"),
                    client_secret: string_field(provider, "clientSecret"),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `bitbucketserver.GetOAuthContext`: first matching `bitbucketserver`
/// `auth.providers` entry; `url` is trimmed of a trailing `/` and
/// prefix-matched against `base_url`. Token URL is
/// `<raw>/rest/oauth2/latest/token`.
pub fn get_oauth_context(
    providers: &[BitbucketServerOAuthProvider],
    base_url: &str,
) -> Option<OAuthContext> {
    for provider in providers {
        let raw_url = provider.url.strip_suffix('/').unwrap_or(&provider.url);
        if !base_url.starts_with(raw_url) {
            continue;
        }
        return Some(OAuthContext {
            client_id: provider.client_id.clone(),
            client_secret: provider.client_secret.clone(),
            token_url: format!("{raw_url}/rest/oauth2/latest/token"),
            custom_query_params: Default::default(),
        });
    }
    None
}

/// `NewProvider`: username-identity provider.
pub struct BitbucketServerProvider {
    urn: String,
    service_id: String,
    client: BitbucketServerClient,
    page_size: i64,
    plugin_perm: bool,
    user_external_account_key: Option<Arc<dyn Key>>,
}

impl BitbucketServerProvider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        client: BitbucketServerClient,
        plugin_perm: bool,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            client,
            page_size: DEFAULT_PAGE_SIZE,
            plugin_perm,
            user_external_account_key: None,
        }
    }

    /// Test seam mirroring the baseline tests' `p.pageSize = 1`.
    pub fn with_page_size(mut self, page_size: i64) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_user_external_account_key(mut self, key: Option<Arc<dyn Key>>) -> Self {
        self.user_external_account_key = key;
        self
    }

    /// `Provider.repoIDs`. The error carries the IDs fetched before the
    /// failing page, which `FetchUserPerms` returns alongside the error.
    async fn repo_ids(
        &self,
        username: &str,
        public: bool,
    ) -> Result<Vec<u32>, (String, Vec<u32>)> {
        if self.plugin_perm {
            return self.repo_ids_from_plugin(username).await;
        }
        self.repo_ids_from_api(username, public).await
    }

    /// `Provider.repoIDsFromAPI`.
    async fn repo_ids_from_api(
        &self,
        username: &str,
        public: bool,
    ) -> Result<Vec<u32>, (String, Vec<u32>)> {
        let mut token = PageToken {
            limit: self.page_size,
            ..PageToken::default()
        };

        let filters: &[&str] = if username.is_empty() {
            &["?visibility=public"]
        } else if !public {
            &["?visibility=private"]
        } else {
            &[]
        };

        let client = if username.is_empty() {
            self.client.clone()
        } else {
            // `repoIDsFromAPI` returns `nil, err` when `Sudo` fails.
            self.client
                .sudo(username)
                .map_err(|err| (err.message, Vec::new()))?
        };

        let mut ids = Vec::new();
        loop {
            let (repos, next) = client
                .repos(&token, filters)
                .await
                .map_err(|err| (err.message, ids.clone()))?;
            ids.extend(repos.into_iter().map(|repo| repo.id as u32));
            if !next.has_more() {
                break;
            }
            token = next;
        }

        if ids.is_empty() {
            // The baseline returns `nil, errNoResults` (no partial IDs).
            return Err((ERR_NO_RESULTS.to_owned(), Vec::new()));
        }
        Ok(ids)
    }

    /// `Provider.repoIDsFromPlugin`.
    async fn repo_ids_from_plugin(
        &self,
        username: &str,
    ) -> Result<Vec<u32>, (String, Vec<u32>)> {
        let client = self
            .client
            .sudo(username)
            .map_err(|err| (err.message, Vec::new()))?;
        client
            .repo_ids("read")
            .await
            .map_err(|err| (err.message, Vec::new()))
    }

    /// `Provider.user`: paged `Users(filter=username)`, exact name match.
    async fn user(&self, username: &str) -> Result<Option<User>, String> {
        let mut token = PageToken {
            limit: self.page_size,
            ..PageToken::default()
        };
        let filters = [UserFilter::by_filter(username)];
        loop {
            let (users, next) = self
                .client
                .users(&token, &filters)
                .await
                .map_err(|err| err.message)?;
            if let Some(user) = users.into_iter().find(|user| user.name == username) {
                return Ok(Some(user));
            }
            if !next.has_more() {
                return Ok(None);
            }
            token = next;
        }
    }

    /// `Provider.userIDs`: paged `Users(Permission{REPO_READ,repoID})`.
    async fn user_ids(&self, repo_id: &str) -> Result<Vec<i64>, String> {
        let mut token = PageToken {
            limit: self.page_size,
            ..PageToken::default()
        };
        let filters = [UserFilter::by_repo_read(repo_id)];
        let mut ids = Vec::new();
        loop {
            let (users, next) = self
                .client
                .users(&token, &filters)
                .await
                .map_err(|err| err.message)?;
            ids.extend(users.into_iter().map(|user| user.id));
            if !next.has_more() {
                return Ok(ids);
            }
            token = next;
        }
    }
}

#[async_trait]
impl AuthzProvider for BitbucketServerProvider {
    fn service_type(&self) -> &str {
        TYPE_BITBUCKETSERVER
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        if !is_host_of(&self.service_id, &repo.service_type, &repo.service_id) {
            return Err(FetchError::Failed {
                message: format!(
                    "not a code host of the repo: want {:?} but have {:?}",
                    self.service_id, repo.service_id
                ),
                partial_account_ids: Vec::new(),
            });
        }

        match self.user_ids(&repo.external_repo_id).await {
            Ok(ids) => Ok(ids.into_iter().map(|id| id.to_string()).collect()),
            Err(message) => Err(FetchError::Failed {
                message,
                partial_account_ids: Vec::new(),
            }),
        }
    }

    async fn fetch_account(
        &self,
        user: &sg_db::users::UserRecord,
    ) -> Result<Option<sg_db::user_external_accounts::ExternalAccountSpec>, String> {
        Ok(self
            .fetch_account_with_data(user)
            .await?
            .map(|fetched| fetched.spec))
    }

    async fn fetch_account_with_data(
        &self,
        user: &sg_db::users::UserRecord,
    ) -> Result<Option<FetchedExternalAccount>, String> {
        let Some(bitbucket_user) = self.user(&user.username).await? else {
            return Ok(None);
        };
        Ok(Some(FetchedExternalAccount {
            spec: sg_db::user_external_accounts::ExternalAccountSpec {
                service_type: TYPE_BITBUCKETSERVER.to_owned(),
                service_id: self.service_id.clone(),
                client_id: String::new(),
                account_id: bitbucket_user.id.to_string(),
            },
            account_data: bitbucket_user.to_account_data_json().into_bytes(),
            auth_data: Vec::new(),
        }))
    }

    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String, partial: Option<ExternalUserPermissions>| {
            UserPermsError::Failed { message, partial }
        };

        // `account.Data == nil`, including the empty cipher with an empty key
        // ID (`extsvc.NewEncryptedData` maps both to nil).
        if encryptable(&account.account_data, &account.encryption_key_id).is_none() {
            return Err(failed("no account data provided".to_owned(), None));
        }
        if !is_host_of(&self.service_id, &account.service_type, &account.service_id) {
            return Err(failed(
                format!(
                    "not a code host of the account: want {:?} but have {:?}",
                    self.service_id, account.service_id
                ),
                None,
            ));
        }

        let user = match self.decode_account_user(account).await {
            Ok(user) => user,
            Err(err) => {
                return Err(failed(format!("unmarshaling account data: {err}"), None));
            }
        };

        match self.repo_ids(&user.name, false).await {
            Ok(ids) => Ok(Some(ExternalUserPermissions {
                exacts: ids.into_iter().map(|id| id.to_string()).collect(),
                ..ExternalUserPermissions::default()
            })),
            // The baseline always returns a non-nil permissions value, built
            // from the IDs fetched before the error.
            Err((message, fetched)) => Err(failed(
                message,
                Some(ExternalUserPermissions {
                    exacts: fetched.into_iter().map(|id| id.to_string()).collect(),
                    ..ExternalUserPermissions::default()
                }),
            )),
        }
    }

    async fn validate_connection(&self) -> Result<(), String> {
        match tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let username = self.client.username().map_err(|err| err.message)?;
            self.client
                .user_permissions(&username)
                .await
                .map_err(|err| err.message)
        })
        .await
        {
            Ok(result) => result.map(|_| ()),
            Err(_) => Err("context deadline exceeded".to_owned()),
        }
    }
}

impl BitbucketServerProvider {
    async fn decode_account_user(&self, account: &ExternalAccount) -> Result<User, String> {
        let data = encryptable(&account.account_data, &account.encryption_key_id)
            .ok_or_else(|| "no account data provided".to_owned())?;
        let plain = sg_encryption::maybe_decrypt(
            self.user_external_account_key(),
            data,
            &account.encryption_key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        parse_go_json::<User>(&plain)
    }

    /// The provider has no keyring field in the baseline (it uses the global
    /// keyring through `encryption.DecryptJSON`); this port reads the same
    /// key the external-account store uses.
    fn user_external_account_key(&self) -> Option<&dyn Key> {
        self.user_external_account_key.as_deref()
    }
}

/// `OAuth2Provider`.
pub struct BitbucketServerOAuth2Provider {
    urn: String,
    service_id: String,
    /// The configured client (connection credentials); user requests use a
    /// bearer-token copy.
    client: BitbucketServerClient,
    page_size: i64,
    plugin_perm: bool,
    user_external_account_key: Option<Arc<dyn Key>>,
}

impl BitbucketServerOAuth2Provider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        client: BitbucketServerClient,
        plugin_perm: bool,
        user_external_account_key: Option<Arc<dyn Key>>,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            client,
            page_size: DEFAULT_PAGE_SIZE,
            plugin_perm,
            user_external_account_key,
        }
    }

    pub fn with_page_size(mut self, page_size: i64) -> Self {
        self.page_size = page_size;
        self
    }

    async fn user_ids(&self, repo_id: &str) -> Result<Vec<i64>, String> {
        let mut token = PageToken {
            limit: self.page_size,
            ..PageToken::default()
        };
        let filters = [UserFilter::by_repo_read(repo_id)];
        let mut ids = Vec::new();
        loop {
            let (users, next) = self
                .client
                .users(&token, &filters)
                .await
                .map_err(|err| err.message)?;
            ids.extend(users.into_iter().map(|user| user.id));
            if !next.has_more() {
                return Ok(ids);
            }
            token = next;
        }
    }

    async fn decode_account_user(&self, account: &ExternalAccount) -> Result<(), String> {
        let Some(data) = encryptable(&account.account_data, &account.encryption_key_id) else {
            return Ok(());
        };
        let plain = sg_encryption::maybe_decrypt(
            self.user_external_account_key.as_deref(),
            data,
            &account.encryption_key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        parse_go_json::<User>(&plain).map(|_| ())
    }
}

#[async_trait]
impl AuthzProvider for BitbucketServerOAuth2Provider {
    fn service_type(&self) -> &str {
        TYPE_BITBUCKETSERVER
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        if !is_host_of(&self.service_id, &repo.service_type, &repo.service_id) {
            return Err(FetchError::Failed {
                message: format!(
                    "not a code host of the repo: want {:?} but have {:?}",
                    self.service_id, repo.service_id
                ),
                partial_account_ids: Vec::new(),
            });
        }

        match self.user_ids(&repo.external_repo_id).await {
            Ok(ids) => Ok(ids.into_iter().map(|id| id.to_string()).collect()),
            Err(message) => Err(FetchError::Failed {
                message,
                partial_account_ids: Vec::new(),
            }),
        }
    }

    /// OAuth2 accounts are created through user sign-in.
    async fn fetch_account(
        &self,
        _user: &sg_db::users::UserRecord,
    ) -> Result<Option<sg_db::user_external_accounts::ExternalAccountSpec>, String> {
        Ok(None)
    }

    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String, partial: Option<ExternalUserPermissions>| {
            UserPermsError::Failed { message, partial }
        };

        if !is_host_of(&self.service_id, &account.service_type, &account.service_id) {
            return Err(failed(
                format!(
                    "not a code host of the account: want {:?} but have {:?}",
                    self.service_id, account.service_id
                ),
                None,
            ));
        }
        if encryptable(&account.account_data, &account.encryption_key_id).is_none() {
            return Err(failed("no account data provided".to_owned(), None));
        }

        // `GetExternalAccountData`: account data first (value discarded).
        if let Err(err) = self.decode_account_user(account).await {
            return Err(failed(err, None));
        }
        let token = match decrypt_stored_token(self.user_external_account_key.as_deref(), account)
            .await
        {
            Ok(token) => token,
            Err(err) => return Err(failed(err, None)),
        };
        // No token means a legacy OAuth1 account.
        let Some(token) = token else {
            return Err(UserPermsError::AccountSuspended {
                message: "account suspended".to_owned(),
            });
        };

        // The baseline passes the token to the Bitbucket Server client as an
        // `auth.OAuthBearerToken`: `(*Client).do` calls only `Authenticate`,
        // so an expired token is used as-is (no pre-emptive refresh and no
        // 401 retry; `oauthutil.DoRequest` is not on this path).
        let client = self.client.with_bearer_token(&token.access_token);
        let ids = match self.fetch_repo_ids_with(&client).await {
            Ok(ids) => ids,
            // The baseline returns the IDs fetched before the failing page
            // alongside the error (`repoIDsFromAPI` returns `ids, err`), and
            // `FetchUserPerms` wraps them in a non-nil permissions value the
            // syncer may use on an initial (`noPerms`) sync.
            Err((err, fetched)) => return Err(classify_error(err.message, err.status, fetched)),
        };
        // `repoIDsFromAPI` returns `errNoResults` as a plain error; keep the
        // (empty) partial result like the baseline. The plugin path
        // (`repoIDsFromPlugin`) has no such check and returns an empty list
        // successfully.
        if ids.is_empty() && !self.plugin_perm {
            return Err(failed(ERR_NO_RESULTS.to_owned(), Some(ExternalUserPermissions::default())));
        }
        let exacts = ids.into_iter().map(|id| id.to_string()).collect();
        Ok(Some(ExternalUserPermissions {
            exacts,
            ..ExternalUserPermissions::default()
        }))
    }

    async fn validate_connection(&self) -> Result<(), String> {
        self.client
            .repos(&PageToken::default(), &[])
            .await
            .map_err(|err| err.message)
            .map(|_| ())
    }
}

impl BitbucketServerOAuth2Provider {
    async fn fetch_repo_ids_with(
        &self,
        client: &BitbucketServerClient,
    ) -> Result<Vec<u32>, (super::bitbucket_server_client::BitbucketServerError, Vec<u32>)> {
        if self.plugin_perm {
            return client
                .repo_ids("read")
                .await
                .map_err(|err| (err, Vec::new()));
        }
        let mut token = PageToken {
            limit: self.page_size,
            ..PageToken::default()
        };
        let mut ids = Vec::new();
        loop {
            let (repos, next) = match client.repos(&token, &["?visibility=private"]).await {
                Ok(page) => page,
                // Baseline `repoIDsFromAPI` returns the IDs collected so far.
                Err(err) => return Err((err, ids)),
            };
            ids.extend(repos.into_iter().map(|repo| repo.id as u32));
            if !next.has_more() {
                return Ok(ids);
            }
            token = next;
        }
    }
}

/// `authz.Provider` errors from `(*Client).do`:
/// `errcode.IsUnauthorized` matches the `httpError`'s `Unauthorized()` method;
/// `httpError` has no `Forbidden()`/`Temporary()` method, so every other
/// status is a plain failure.
fn classify_error(message: String, status: Option<u16>, partial_ids: Vec<u32>) -> UserPermsError {
    match status {
        Some(401) => UserPermsError::Unauthorized { message },
        _ => UserPermsError::Failed {
            message,
            // Baseline `FetchUserPerms` always returns a non-nil permissions
            // value, even alongside the error.
            partial: Some(ExternalUserPermissions {
                exacts: partial_ids.into_iter().map(|id| id.to_string()).collect(),
                ..ExternalUserPermissions::default()
            }),
        },
    }
}

/// `extsvc.IsHostOfAccount` / `IsHostOfRepo`.
fn is_host_of(provider_service_id: &str, service_type: &str, service_id: &str) -> bool {
    service_type == TYPE_BITBUCKETSERVER && service_id == provider_service_id
}

/// `extsvc.NewEncryptedData` nil semantics.
fn encryptable<'a>(data: &'a Option<String>, key_id: &str) -> Option<&'a str> {
    let data = data.as_deref()?;
    if data.is_empty() && key_id.is_empty() {
        return None;
    }
    Some(data)
}

/// `json.Unmarshal` into a Go struct: `null` decodes to the zero value and an
/// empty input reports Go's `unexpected end of JSON input` text.
fn parse_go_json<T: for<'de> serde::Deserialize<'de> + Default>(plain: &str) -> Result<T, String> {
    if plain.trim().is_empty() {
        return Err("unexpected end of JSON input".to_owned());
    }
    let value: Option<T> = serde_json::from_str(plain).map_err(|err| err.to_string())?;
    Ok(value.unwrap_or_default())
}

/// `newAuthzProvider` for one connection.
pub fn new_bitbucketserver_authz_provider(
    conn: &BitbucketServerConnection,
    urn: &str,
    service_id: &str,
    plugin_perm: bool,
    acls_license_error: Option<&str>,
    key: Option<Arc<dyn Key>>,
    http: reqwest::Client,
) -> Result<Option<Arc<dyn AuthzProvider>>, String> {
    let Some(authorization) = &conn.authorization else {
        return Ok(None);
    };
    if !authorization.oauth2 && !authorization.has_identity_provider() {
        return Err("authorization is set without oauth2 or identityProvider".to_owned());
    }
    // `licensing.Check(licensing.FeatureACLs)` happens after the authorization
    // gate and before the client is constructed.
    if let Some(message) = acls_license_error {
        return Err(message.to_owned());
    }

    let client = BitbucketServerClient::new(conn, http.clone()).map_err(|err| err.message)?;

    if authorization.oauth2 {
        Ok(Some(Arc::new(BitbucketServerOAuth2Provider::new(
            urn,
            service_id,
            client,
            plugin_perm,
            key,
        ))))
    } else if authorization.username_identity() {
        Ok(Some(Arc::new(BitbucketServerProvider::new(
            urn,
            service_id,
            client,
            plugin_perm,
        ))))
    } else {
        Err("No identityProvider was specified".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn oauth_providers_filter_bitbucketserver_entries() {
        let site = site(
            r#"{"auth.providers":[
                {"type":"gitlab","url":"https://gitlab.example.com"},
                {"type":"bitbucketserver","url":"https://bbs.example.com/","clientID":"id","clientSecret":"secret"}
            ]}"#,
        );
        assert_eq!(
            bitbucketserver_oauth_providers(&site),
            vec![BitbucketServerOAuthProvider {
                url: "https://bbs.example.com/".to_owned(),
                client_id: "id".to_owned(),
                client_secret: "secret".to_owned(),
            }]
        );
    }

    #[test]
    fn oauth_context_trims_trailing_slash_and_prefix_matches() {
        let providers = vec![BitbucketServerOAuthProvider {
            url: "https://bbs.example.com/".to_owned(),
            client_id: "id".to_owned(),
            client_secret: "secret".to_owned(),
        }];
        let context = get_oauth_context(&providers, "https://bbs.example.com/").expect("context");
        assert_eq!(context.token_url, "https://bbs.example.com/rest/oauth2/latest/token");
        assert!(get_oauth_context(&providers, "https://other.example.com/").is_none());
    }

    #[test]
    fn go_json_parsing_matches_json_unmarshal() {
        assert_eq!(parse_go_json::<User>("null").expect("null").name, "");
        assert_eq!(
            parse_go_json::<User>("").expect_err("empty"),
            "unexpected end of JSON input"
        );
    }

    #[test]
    fn no_authorization_builds_no_provider() {
        let conn = BitbucketServerConnection {
            url: "https://bbs.example.com/".to_owned(),
            ..Default::default()
        };
        assert!(new_bitbucketserver_authz_provider(
            &conn,
            "extsvc:bitbucketserver:1",
            "https://bbs.example.com/",
            false,
            None,
            None,
            reqwest::Client::new(),
        )
        .expect("no error")
        .is_none());
    }

    #[test]
    fn authorization_without_oauth2_or_identity_provider_is_an_error() {
        let conn: BitbucketServerConnection = serde_json::from_str(
            r#"{"url":"https://bbs.example.com/","authorization":{}}"#,
        )
        .expect("config");
        let err = match new_bitbucketserver_authz_provider(
            &conn,
            "extsvc:bitbucketserver:1",
            "https://bbs.example.com/",
            false,
            None,
            None,
            reqwest::Client::new(),
        ) {
            Ok(_) => panic!("expected an error"),
            Err(err) => err,
        };
        assert_eq!(err, "authorization is set without oauth2 or identityProvider");
    }
}
