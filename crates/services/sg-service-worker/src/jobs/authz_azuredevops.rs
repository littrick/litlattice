#![forbid(unsafe_code)]

//! `internal/authz/providers/azuredevops`: the Azure DevOps authz provider.
//!
//! * `NewAuthzProviders` / `newAuthzProvider` (`provider.go`): every
//!   `enforcePermissions` connection is merged into one provider whose
//!   `orgs`/`projects` sets drive which of the user's organizations are
//!   synced. The license check and connection construction are wired in
//!   [`super::authz_providers`].
//! * `Provider.FetchUserPerms`: decrypt the external account's profile and
//!   OAuth token, refresh-and-store the token through
//!   [`super::authz_github_user`], then enumerate the user's organizations
//!   (`_apis/accounts`) and, for every configured org, the repositories the
//!   user can read (`{org}/_apis/git/repositories`). A 401/403/404 for an org
//!   means the user has no access and is skipped; any other failure is fatal.
//! * `Provider.FetchRepoPerms`: `authz.ErrUnimplemented{Feature:
//!   "azuredevops.FetchRepoPerms"}`.
//! * `Provider.ValidateConnection`: one Basic-auth profile request per
//!   connection, joined into the baseline's multi-error message.
//!
//! `FetchAccount` returns `nil` (the default `Ok(None)`): accounts are created
//! by the OAuth sign-in flow.
//!
//! Not ported: the 5s validation timeout, the per-request rate limiters and
//! the warning logs (see [`super::authz_azuredevops_client`]).

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use sg_db::user_external_accounts::ExternalAccount;
use sg_encryption::Key;

use super::authz_azuredevops_client::{
    get_oauth_context, AzureDevOpsAuth, AzureDevOpsError, AzureDevOpsOAuthProvider,
    AzureDevOpsRestClient, AZURE_DEVOPS_API_URL,
};
use super::authz_github_user::{
    decrypt_stored_token, AccountTokenRefresher, OAuthBearerToken, OAuthTokenStore,
};
use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    UserPermsError,
};

/// `extsvc.TypeAzureDevOps`.
pub const TYPE_AZUREDEVOPS: &str = "azuredevops";
/// `newAuthzProvider`'s fixed URN.
pub const URN: &str = "azuredevops:authzprovider";
/// `extsvc.NewCodeHost(url.Parse(AzureDevOpsAPIURL), TypeAzureDevOps).ServiceID`.
pub const SERVICE_ID: &str = "https://dev.azure.com/";

/// `types.AzureDevOpsConnection`, reduced to the fields the provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AzureDevOpsConnection {
    pub urn: String,
    pub username: String,
    pub token: String,
    pub orgs: Vec<String>,
    pub projects: Vec<String>,
}

/// Constructor inputs the baseline reads from package/global state: the API
/// URL, the `MockVisualStudioAppURL` seam, the site's `auth.providers` and
/// `externalURL`.
#[derive(Debug, Clone)]
pub struct AzureDevOpsProviderOptions {
    pub api_url: String,
    pub visual_studio_app_url: Option<reqwest::Url>,
    pub oauth_providers: Vec<AzureDevOpsOAuthProvider>,
    pub external_url: String,
}

impl Default for AzureDevOpsProviderOptions {
    fn default() -> Self {
        Self {
            api_url: AZURE_DEVOPS_API_URL.to_owned(),
            visual_studio_app_url: None,
            oauth_providers: Vec::new(),
            external_url: String::new(),
        }
    }
}

/// `azuredevops.Provider`.
pub struct AzureDevOpsProvider {
    conns: Vec<AzureDevOpsConnection>,
    /// The deduplicated `orgs` across the merged connections.
    orgs: BTreeSet<String>,
    /// The deduplicated `org/project` strings across the merged connections.
    projects: BTreeSet<String>,
    options: AzureDevOpsProviderOptions,
    token_store: Arc<dyn OAuthTokenStore>,
    user_external_account_key: Option<Arc<dyn Key>>,
    http: reqwest::Client,
}

impl AzureDevOpsProvider {
    pub fn new(
        conns: Vec<AzureDevOpsConnection>,
        options: AzureDevOpsProviderOptions,
        token_store: Arc<dyn OAuthTokenStore>,
        user_external_account_key: Option<Arc<dyn Key>>,
        http: reqwest::Client,
    ) -> Result<Self, String> {
        // `newAuthzProvider`: parse the API URL constant. The baseline can
        // only fail here on a misconfigured constant.
        reqwest::Url::parse(&options.api_url).map_err(|err| {
            format!(
                "failed to parse url: {:?}, this is likely a misconfigured URL in the constant azuredevops.AzureDevOpsAPIURL: {err}",
                options.api_url
            )
        })?;

        let mut orgs = BTreeSet::new();
        let mut projects = BTreeSet::new();
        for conn in &conns {
            orgs.extend(conn.orgs.iter().cloned());
            projects.extend(conn.projects.iter().cloned());
        }

        Ok(Self {
            conns,
            orgs,
            projects,
            options,
            token_store,
            user_external_account_key,
            http,
        })
    }
}

#[async_trait]
impl AuthzProvider for AzureDevOpsProvider {
    fn service_type(&self) -> &str {
        TYPE_AZUREDEVOPS
    }

    fn service_id(&self) -> &str {
        SERVICE_ID
    }

    fn urn(&self) -> &str {
        URN
    }

    /// `FetchRepoPerms`: user-centric syncing only.
    async fn fetch_repo_perms(
        &self,
        _repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        Err(FetchError::Unimplemented)
    }

    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String| UserPermsError::Failed {
            message,
            partial: None,
        };

        let (profile, token) = self
            .external_account_data(account)
            .await
            .map_err(|err| {
                failed(format!(
                    "failed to load external account data from database with external account with ID: {}: {err}",
                    account.id
                ))
            })?;

        // The baseline dereferences the nil token here; Rust reports an error
        // instead (docs §73.3).
        let Some(token) = token else {
            return Err(failed(
                "no token found in the external account data".to_owned(),
            ));
        };

        let oauth_context = get_oauth_context(
            &self.options.oauth_providers,
            &self.options.external_url,
            &token.refresh_token,
        )
        .map_err(|err| {
            failed(format!(
                "failed to generate oauth context, this is likely a misconfiguration with the Azure OAuth provider (bad URL?), please check the auth.providers configuration in your site config: {err}"
            ))
        })?;

        let refresher = Arc::new(AccountTokenRefresher::new(
            self.token_store.clone(),
            self.user_external_account_key.clone(),
            account.id,
            oauth_context,
            self.http.clone(),
        ));
        let bearer = Arc::new(OAuthBearerToken::new(token, Some(refresher)));

        let client = AzureDevOpsRestClient::new(
            &self.options.api_url,
            AzureDevOpsAuth::Bearer(bearer),
            self.http.clone(),
        )
        .map_err(|err| {
            failed(format!(
                "failed to create client for service ID: {:?}, account ID: {:?}: {err}",
                SERVICE_ID, account.account_id
            ))
        })?
        .with_visual_studio_app_url(self.options.visual_studio_app_url.clone());

        let user_profile = match profile {
            Some(profile) => profile,
            None => client
                .get_authorized_profile()
                .await
                .map_err(classify_user_error)?,
        };

        // Always list the orgs accessible by this user: the code host config
        // may only name projects.
        let orgs = client
            .list_authorized_user_organizations(&user_profile)
            .await
            .map_err(classify_user_error)?;

        // `allOrgs`: the configured orgs plus every project's org. A project
        // that does not split into exactly two parts is skipped (the baseline
        // logs a warning).
        let mut all_orgs = self.orgs.clone();
        for project in &self.projects {
            let parts: Vec<&str> = project.split('/').collect();
            if parts.len() != 2 {
                continue;
            }
            all_orgs.insert(parts[0].to_owned());
        }

        let mut repos = Vec::new();
        for org in orgs {
            if !all_orgs.contains(&org.name) {
                continue;
            }

            match client.list_repositories_by_project_or_org(&org.name).await {
                Ok(found) => repos.extend(found),
                // 401 / 403 / 404: the user has no access to this org.
                Err(err)
                    if err.status == Some(401) || err.status == Some(403) || err.not_found() =>
                {
                    continue;
                }
                Err(err) => {
                    return Err(failed(format!(
                        "failed to list repositories for org: {} with error: {:?}",
                        go_quoted_org(&org),
                        err.message
                    )));
                }
            }
        }

        Ok(Some(ExternalUserPermissions {
            exacts: repos.into_iter().map(|repo| repo.id).collect(),
            ..ExternalUserPermissions::default()
        }))
    }

    /// `ValidateConnection`: a Basic-auth profile request per connection.
    async fn validate_connection(&self) -> Result<(), String> {
        let mut all_errors = Vec::new();
        for conn in &self.conns {
            let client = match AzureDevOpsRestClient::new(
                AZURE_DEVOPS_API_URL,
                AzureDevOpsAuth::Basic {
                    username: conn.username.clone(),
                    password: conn.token.clone(),
                },
                self.http.clone(),
            ) {
                Ok(client) => {
                    client.with_visual_studio_app_url(self.options.visual_studio_app_url.clone())
                }
                Err(err) => {
                    all_errors.push(format!("{}:{}", conn.urn, err));
                    continue;
                }
            };

            if let Err(err) = client.get_authorized_profile().await {
                all_errors.push(err.message);
            }
        }

        if !all_errors.is_empty() {
            return Err(format!(
                "ValidateConnection failed for Azure DevOps with the following errors: {}",
                all_errors.join("\n")
            ));
        }

        Ok(())
    }
}

impl AzureDevOpsProvider {
    /// `azuredevops.GetExternalAccountData`: the profile (`account_data`) and
    /// token (`auth_data`), decoded in that order.
    async fn external_account_data(
        &self,
        account: &ExternalAccount,
    ) -> Result<
        (
            Option<super::authz_azuredevops_client::Profile>,
            Option<super::authz_github_user::OAuthToken>,
        ),
        String,
    > {
        let profile = match encryptable(&account.account_data, &account.encryption_key_id) {
            None => None,
            Some(data) => {
                let plain = sg_encryption::maybe_decrypt(
                    self.user_external_account_key.as_deref(),
                    data,
                    &account.encryption_key_id,
                )
                .await
                .map_err(|err| err.to_string())?;
                serde_json::from_str::<Option<super::authz_azuredevops_client::Profile>>(&plain)
                    .map_err(|err| err.to_string())?
            }
        };
        let token =
            decrypt_stored_token(self.user_external_account_key.as_deref(), account).await?;
        Ok((profile, token))
    }
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

/// `fmt.Sprintf("%q", org)` on a three-field struct.
fn go_quoted_org(org: &super::authz_azuredevops_client::Org) -> String {
    format!("{{{:?} {:?} {:?}}}", org.id, org.uri, org.name)
}

/// `errcode.IsUnauthorized` sees `HTTPError.Unauthorized()`; every other
/// failure is a plain `Failed`.
fn classify_user_error(err: AzureDevOpsError) -> UserPermsError {
    if err.unauthorized() {
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
    use async_trait::async_trait;

    #[test]
    fn go_quoted_org_matches_fmt_q() {
        let org = super::super::authz_azuredevops_client::Org {
            id: "1".to_owned(),
            uri: String::new(),
            name: "solarsystem".to_owned(),
        };
        assert_eq!(go_quoted_org(&org), r#"{"1" "" "solarsystem"}"#);
    }

    #[test]
    fn invalid_api_url_is_a_construction_error() {
        let options = AzureDevOpsProviderOptions {
            api_url: "://bad".to_owned(),
            ..AzureDevOpsProviderOptions::default()
        };
        let err = AzureDevOpsProvider::new(
            Vec::new(),
            options,
            Arc::new(NoopTokenStore),
            None,
            reqwest::Client::new(),
        )
        .err()
        .expect("invalid URL");
        assert!(err.starts_with("failed to parse url: \"://bad\", this is likely a misconfigured URL in the constant azuredevops.AzureDevOpsAPIURL: "));
    }

    struct NoopTokenStore;

    #[async_trait]
    impl OAuthTokenStore for NoopTokenStore {
        async fn get(&self, _id: i32) -> Result<Option<ExternalAccount>, String> {
            Ok(None)
        }

        async fn update(&self, _account: &ExternalAccount, _auth_data: &str) -> Result<(), String> {
            Ok(())
        }
    }
}
