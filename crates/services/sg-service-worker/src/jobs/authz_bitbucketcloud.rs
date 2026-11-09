#![forbid(unsafe_code)]

//! `internal/authz/providers/bitbucketcloud`: the Bitbucket Cloud authz
//! provider.
//!
//! * `NewAuthzProviders` / `newAuthzProvider` (`authz.go`): the authorization
//!   gate, ACLs license check and client construction are wired in
//!   [`super::authz_providers`]; this module owns the provider itself.
//! * `Provider.FetchUserPerms` (`provider.go`): decrypt `account_data` (as a
//!   Bitbucket Cloud `Account`, value discarded) and `auth_data` (as an OAuth
//!   token), build `auth.OAuthBearerToken` with
//!   `GetAccountRefreshAndStoreOAuthTokenFunc(…, bitbucketcloud.GetOAuthContext(baseURL))`,
//!   then list the repositories the token is a member of (`role=member`,
//!   `FetchAll`).
//! * `Provider.FetchRepoPerms`: list the explicit user permissions of
//!   `bitbucket.org/<owner>/<repo>` plus the repository owner.
//! * `bitbucketcloud.GetOAuthContext`: prefix match over the site's
//!   `auth.providers` entries of type `bitbucketcloud` (empty `url` defaults
//!   to `https://bitbucket.org`).
//!
//! Not ported: the client's self-imposed rate limiter, its 429 back-off and
//! request counter (see [`super::authz_bitbucketcloud_client`]).

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::user_external_accounts::ExternalAccount;
use sg_encryption::Key;

pub use super::authz_bitbucketcloud_client::TYPE_BITBUCKETCLOUD;
use super::authz_bitbucketcloud_client::{BitbucketCloudClient, BitbucketCloudError};
use super::authz_github_user::{
    decrypt_stored_token, AccountTokenRefresher, OAuthBearerToken, OAuthContext, OAuthTokenStore,
    TokenRefresher,
};
use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    UserPermsError,
};

/// One `auth.providers` entry of type `bitbucketcloud`, reduced to the fields
/// `GetOAuthContext` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketCloudOAuthProvider {
    pub url: String,
    pub client_key: String,
    pub client_secret: String,
}

/// `conf.SiteConfig().AuthProviders` entries of type `bitbucketcloud`.
pub fn bitbucketcloud_oauth_providers(
    site: &SiteConfiguration,
) -> Vec<BitbucketCloudOAuthProvider> {
    site.raw()
        .get("auth.providers")
        .and_then(serde_json::Value::as_array)
        .map(|providers| {
            providers
                .iter()
                .filter(|provider| {
                    provider.get("type").and_then(serde_json::Value::as_str)
                        == Some("bitbucketcloud")
                })
                .map(|provider| BitbucketCloudOAuthProvider {
                    url: string_field(provider, "url"),
                    client_key: string_field(provider, "clientKey"),
                    client_secret: string_field(provider, "clientSecret"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `bitbucketcloud.GetOAuthContext`: the first entry whose URL (trailing slash
/// trimmed) prefixes `base_url`, with the empty URL defaulting to
/// `https://bitbucket.org`. `url.JoinPath` is reproduced by concatenating the
/// token path (the URL has no trailing slash after the trim).
pub fn get_oauth_context(
    providers: &[BitbucketCloudOAuthProvider],
    base_url: &str,
) -> Option<OAuthContext> {
    for provider in providers {
        let raw_url = if provider.url.is_empty() {
            "https://bitbucket.org"
        } else {
            &provider.url
        };
        let raw_url = raw_url.strip_suffix('/').unwrap_or(raw_url);
        if !base_url.starts_with(raw_url) {
            continue;
        }
        return Some(OAuthContext {
            client_id: provider.client_key.clone(),
            client_secret: provider.client_secret.clone(),
            token_url: format!("{raw_url}/site/oauth2/access_token"),
            custom_query_params: std::collections::BTreeMap::new(),
        });
    }
    None
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `bitbucketcloud.Provider`.
pub struct BitbucketCloudProvider {
    urn: String,
    /// `extsvc.NewCodeHost(conn.Url, "bitbucketcloud").ServiceID`.
    service_id: String,
    /// The configured client (connection credentials); `WithAuthenticator`
    /// derives the user-token client from it.
    client: Arc<dyn BitbucketCloudClient>,
    /// The user-external-account keyring `GetExternalAccountData` decrypts
    /// `account_data`/`auth_data` with.
    user_external_account_key: Option<Arc<dyn Key>>,
    /// The store the token refresher re-reads and updates.
    token_store: Arc<dyn OAuthTokenStore>,
    /// The HTTP client the refresher posts with (the same one the API client
    /// uses, like `oauthutil.DoRequest` passing the client's doer).
    http: reqwest::Client,
    /// `conf.SiteConfig().AuthProviders` entries of type `bitbucketcloud`.
    oauth_providers: Vec<BitbucketCloudOAuthProvider>,
}

impl BitbucketCloudProvider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        client: Arc<dyn BitbucketCloudClient>,
        token_store: Arc<dyn OAuthTokenStore>,
        http: reqwest::Client,
        oauth_providers: Vec<BitbucketCloudOAuthProvider>,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            client,
            user_external_account_key: None,
            token_store,
            http,
            oauth_providers,
        }
    }

    pub fn with_user_external_account_key(mut self, key: Option<Arc<dyn Key>>) -> Self {
        self.user_external_account_key = key;
        self
    }
}

#[async_trait]
impl AuthzProvider for BitbucketCloudProvider {
    fn service_type(&self) -> &str {
        TYPE_BITBUCKETCLOUD
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    /// `FetchRepoPerms`: the explicit users of the repository plus its owner.
    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        let failed = |message: String| FetchError::Failed {
            message,
            partial_account_ids: Vec::new(),
        };

        // `strings.Split(repo.URI, "/")` followed by index 1 and 2: the
        // baseline panics on a malformed URI, Rust reports an error instead
        // (docs §71.3).
        let parts: Vec<&str> = repo.uri.split('/').collect();
        let (Some(owner), Some(name)) = (parts.get(1), parts.get(2)) else {
            return Err(failed(format!(
                "invalid repository URI {:?}: expected <host>/<owner>/<name>",
                repo.uri
            )));
        };

        // The syncer's `github.APIError` 404 special case does not apply to
        // Bitbucket Cloud's `httpError`, so every failure is a plain error
        // (docs §71.3).
        let users = self
            .client
            .list_explicit_user_perms_for_repo(owner, name, true)
            .await
            .map_err(|err| failed(err.message))?;

        // The API omits the repository owner from the explicit permissions
        // list, so the baseline fetches and appends it.
        let repository = self
            .client
            .repo(owner, name)
            .await
            .map_err(|err| failed(err.message))?;

        let mut account_ids: Vec<String> = users.into_iter().map(|user| user.uuid).collect();
        if let Some(owner) = repository.owner {
            account_ids.push(owner.uuid);
        }
        Ok(account_ids)
    }

    /// `FetchUserPerms`: the repositories the user's stored OAuth token is a
    /// member of. `oauthutil.DoRequest` refreshes an expiring token, and one
    /// 401 triggers a refresh-and-retry.
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String| UserPermsError::Failed {
            message,
            partial: None,
        };

        // Unlike GitHub/GitLab, the Bitbucket Cloud baseline words the check
        // with the provider first (`want`), the account second (`have`).
        if account.service_id != self.service_id || account.service_type != TYPE_BITBUCKETCLOUD {
            return Err(failed(format!(
                "not a code host of the account: want {:?} but have {:?}",
                self.service_id, account.service_id
            )));
        }

        // `account.Data == nil` (including the empty cipher + empty key ID).
        if encryptable_account_data(account).is_none() {
            return Err(failed("no account data provided".to_owned()));
        }

        // `bitbucketcloud.GetExternalAccountData` decodes the account data as
        // an `Account` (its value is discarded) before reading the token; a
        // decryption/decoding error is returned unwrapped.
        if let Err(err) = self.decode_account_data(account).await {
            return Err(failed(err));
        }
        let token =
            match decrypt_stored_token(self.user_external_account_key.as_deref(), account).await {
                Ok(token) => token,
                Err(err) => return Err(failed(err)),
            };
        // The baseline dereferences a nil token here; Rust reports an error
        // instead (docs §71.3).
        let Some(token) = token else {
            return Err(failed(
                "no token found in the external account data".to_owned(),
            ));
        };

        // `bitbucketcloud.GetOAuthContext(p.codeHost.BaseURL.String())`. The
        // code host URL is already normalized (the service ID).
        let refresher: Option<Arc<dyn TokenRefresher>> =
            get_oauth_context(&self.oauth_providers, &self.service_id).map(|oauth| {
                Arc::new(AccountTokenRefresher::new(
                    self.token_store.clone(),
                    self.user_external_account_key.clone(),
                    account.id,
                    oauth,
                    self.http.clone(),
                )) as Arc<dyn TokenRefresher>
            });
        let bearer = Arc::new(OAuthBearerToken::new(token, refresher));
        let client = self.client.with_bearer_token(bearer);

        // `Repos(ctx, &PageToken{Pagelen: 100}, "", &ReposOptions{
        // RequestOptions: {FetchAll: true}, Role: "member"})`.
        let repos = match client.repos("", Some("member"), true).await {
            Ok(repos) => repos,
            Err(err) => return Err(classify_user_error(err)),
        };

        Ok(Some(ExternalUserPermissions {
            exacts: repos.into_iter().map(|repo| repo.uuid).collect(),
            ..ExternalUserPermissions::default()
        }))
    }
}

impl BitbucketCloudProvider {
    async fn decode_account_data(&self, account: &ExternalAccount) -> Result<(), String> {
        let Some(data) = encryptable_account_data(account) else {
            return Ok(());
        };
        let plain = sg_encryption::maybe_decrypt(
            self.user_external_account_key.as_deref(),
            data,
            &account.encryption_key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        // `json.Unmarshal` into a struct pointer accepts a JSON `null`.
        serde_json::from_str::<Option<super::authz_bitbucketcloud_client::Account>>(&plain)
            .map_err(|err| err.to_string())?;
        Ok(())
    }
}

/// `GetExternalAccountData`'s `Data != nil` check, including the empty cipher
/// with an empty key ID (which `extsvc.NewEncryptedData` maps to nil).
fn encryptable_account_data(account: &ExternalAccount) -> Option<&str> {
    let data = account.account_data.as_deref()?;
    if data.is_empty() && account.encryption_key_id.is_empty() {
        return None;
    }
    Some(data)
}

/// `errcode.IsUnauthorized` sees the client's `Unauthorized() bool` method
/// (HTTP 401); every other failure is a plain `Failed`.
fn classify_user_error(err: BitbucketCloudError) -> UserPermsError {
    if err.status == Some(401) {
        UserPermsError::Unauthorized {
            message: err.message,
        }
    } else {
        UserPermsError::Failed {
            message: err.message,
            partial: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn oauth_providers_filter_bitbucketcloud_entries() {
        let site = site(
            r#"{"auth.providers":[
                {"type":"gitlab","url":"https://gitlab.example.com"},
                {"type":"bitbucketcloud","url":"https://bitbucket.org/","clientKey":"key","clientSecret":"secret"}
            ]}"#,
        );
        assert_eq!(
            bitbucketcloud_oauth_providers(&site),
            vec![BitbucketCloudOAuthProvider {
                url: "https://bitbucket.org/".to_owned(),
                client_key: "key".to_owned(),
                client_secret: "secret".to_owned(),
            }]
        );
    }

    #[test]
    fn oauth_context_defaults_and_prefix_matches() {
        let providers = vec![BitbucketCloudOAuthProvider {
            url: String::new(),
            client_key: "key".to_owned(),
            client_secret: "secret".to_owned(),
        }];
        let context = get_oauth_context(&providers, "https://bitbucket.org/").expect("context");
        assert_eq!(context.client_id, "key");
        assert_eq!(
            context.token_url,
            "https://bitbucket.org/site/oauth2/access_token"
        );

        let providers = vec![BitbucketCloudOAuthProvider {
            url: "https://bb.example.com/".to_owned(),
            client_key: "other".to_owned(),
            client_secret: "s".to_owned(),
        }];
        assert!(get_oauth_context(&providers, "https://bitbucket.org/").is_none());
        let context = get_oauth_context(&providers, "https://bb.example.com/").expect("context");
        assert_eq!(
            context.token_url,
            "https://bb.example.com/site/oauth2/access_token"
        );
    }
}
