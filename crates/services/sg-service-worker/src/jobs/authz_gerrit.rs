#![forbid(unsafe_code)]

//! `internal/authz/providers/gerrit`: the Gerrit authz provider.
//!
//! * `NewAuthzProviders` / `NewProvider` (`authz.go`, `gerrit.go`): the
//!   authorization gate, ACLs license check and client construction are wired
//!   in [`super::authz_providers`]; this module owns the provider itself.
//! * `Provider.FetchUserPerms`: decrypt the account's `auth_data` as
//!   `gerrit.AccountCredentials`, swap the connection's Basic authenticator
//!   for the user's, then page through every project
//!   (`a/projects/?S=<skip>&n=100`) and return the project IDs as the
//!   account's exact repositories.
//! * `Provider.FetchRepoPerms`: `authz.ErrUnimplemented{Feature:
//!   "gerrit.FetchRepoPerms"}`.
//! * `Provider.ValidateConnection`: `GetGroup("Administrators")` with the
//!   connection credentials and the three field checks that prove the
//!   credentials have administrator scope.
//!
//! `FetchAccount` is unused for Gerrit (the default `Ok(None)`); users supply
//! their own credentials through the external account's auth data.
//!
//! Not ported: the client's self-imposed rate limiter (see
//! [`super::authz_gerrit_client`]).

use std::sync::Arc;

use async_trait::async_trait;
use sg_db::user_external_accounts::ExternalAccount;
use sg_encryption::Key;

pub use super::authz_gerrit_client::TYPE_GERRIT;
use super::authz_gerrit_client::{GerritClient, GerritError};
use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    UserPermsError,
};

/// `adminGroupName`.
pub const ADMIN_GROUP_NAME: &str = "Administrators";

/// `gerrit.AccountCredentials`: the user's HTTP Basic credentials, stored
/// encrypted in the external account's `auth_data`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct AccountCredentials {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
}

/// `gerrit.Provider`.
pub struct GerritProvider {
    urn: String,
    /// `extsvc.NewCodeHost(conn.Url, "gerrit").ServiceID`.
    service_id: String,
    /// The connection-level client; `with_basic_auth` derives the user-client
    /// from it, like `WithAuthenticator`.
    client: Arc<dyn GerritClient>,
    /// The user-external-account keyring `GetExternalAccountCredentials`
    /// decrypts `auth_data` with.
    user_external_account_key: Option<Arc<dyn Key>>,
}

impl GerritProvider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        client: Arc<dyn GerritClient>,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            client,
            user_external_account_key: None,
        }
    }

    pub fn with_user_external_account_key(mut self, key: Option<Arc<dyn Key>>) -> Self {
        self.user_external_account_key = key;
        self
    }
}

#[async_trait]
impl AuthzProvider for GerritProvider {
    fn service_type(&self) -> &str {
        TYPE_GERRIT
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    /// `FetchRepoPerms`: not implemented by the Gerrit provider.
    async fn fetch_repo_perms(
        &self,
        _repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        Err(FetchError::Unimplemented)
    }

    /// `FetchUserPerms`: every project the user's Gerrit credentials can list.
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String| UserPermsError::Failed {
            message,
            partial: None,
        };

        // `extsvc.IsHostOfAccount`: service ID and type must both match.
        if account.service_id != self.service_id || account.service_type != TYPE_GERRIT {
            return Err(failed(format!(
                "not a code host of the account: want {:?} but have {:?}",
                account.service_id, self.service_id
            )));
        }

        // `account.Data == nil || account.AuthData == nil`.
        if encryptable(&account.account_data, &account.encryption_key_id).is_none()
            || encryptable(&account.auth_data, &account.encryption_key_id).is_none()
        {
            return Err(failed("no account data".to_owned()));
        }

        let credentials = match self.account_credentials(account).await {
            Ok(credentials) => credentials,
            Err(err) => return Err(failed(err)),
        };

        // `WithAuthenticator(&auth.BasicAuth{...})`. Every credential pair is
        // a valid Basic authenticator, so the baseline's unsupported-type
        // error cannot occur here.
        let client = self
            .client
            .with_basic_auth(&credentials.username, &credentials.password);

        // `Pagination{PerPage: 100, Page: 1}`, incremented until a page is
        // not full (`len(projects) == PerPage`).
        let mut page = 1_i64;
        let mut exacts = Vec::new();
        loop {
            let projects = match client.list_projects(100, page).await {
                Ok(projects) => projects,
                Err(err) => return Err(classify_user_error(err)),
            };
            for project in projects.projects.into_values().flatten() {
                exacts.push(project.id);
            }
            if !projects.next_page {
                break;
            }
            page += 1;
        }

        Ok(Some(ExternalUserPermissions {
            exacts,
            ..ExternalUserPermissions::default()
        }))
    }

    /// `ValidateConnection`: prove the connection credentials can read the
    /// `Administrators` group.
    async fn validate_connection(&self) -> Result<(), String> {
        let admin_group = self
            .client
            .get_group(ADMIN_GROUP_NAME)
            .await
            .map_err(|err| format!("Unable to get {ADMIN_GROUP_NAME} group: {}", err.message))?;

        if admin_group.id.is_empty()
            || admin_group.name != ADMIN_GROUP_NAME
            || admin_group.created_on.is_empty()
        {
            // The baseline's "sufficent" typo is preserved.
            return Err(format!(
                "Gerrit credentials not sufficent enough to query {ADMIN_GROUP_NAME} group"
            ));
        }

        Ok(())
    }
}

impl GerritProvider {
    /// `gerrit.GetExternalAccountCredentials`: decrypt `auth_data` as
    /// [`AccountCredentials`]. A JSON `null` decodes as the zero value, like
    /// `encryption.DecryptJSON` unmarshalling into a struct pointer.
    async fn account_credentials(
        &self,
        account: &ExternalAccount,
    ) -> Result<AccountCredentials, String> {
        let Some(data) = encryptable(&account.auth_data, &account.encryption_key_id) else {
            return Err("no account data".to_owned());
        };
        let plain = sg_encryption::maybe_decrypt(
            self.user_external_account_key.as_deref(),
            data,
            &account.encryption_key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        let credentials = serde_json::from_str::<Option<AccountCredentials>>(&plain)
            .map_err(|err| err.to_string())?;
        Ok(credentials.unwrap_or_default())
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

/// `errcode.IsUnauthorized` sees `httpError.Unauthorized()` (HTTP 401); every
/// other failure is a plain `Failed`.
fn classify_user_error(err: GerritError) -> UserPermsError {
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

    #[test]
    fn credentials_decode_and_default() {
        let credentials: Option<AccountCredentials> =
            serde_json::from_str(r#"{"username":"u","password":"p"}"#).expect("decode");
        assert_eq!(
            credentials,
            Some(AccountCredentials {
                username: "u".to_owned(),
                password: "p".to_owned(),
            })
        );
        let credentials: Option<AccountCredentials> = serde_json::from_str("null").expect("decode");
        assert_eq!(credentials, None);
    }

    #[test]
    fn encryptable_matches_nil_cipher_semantics() {
        assert_eq!(encryptable(&None, ""), None);
        assert_eq!(encryptable(&Some(String::new()), ""), None);
        assert_eq!(encryptable(&Some("cipher".to_owned()), ""), Some("cipher"));
        assert_eq!(encryptable(&Some(String::new()), "kid"), Some(""));
    }
}
