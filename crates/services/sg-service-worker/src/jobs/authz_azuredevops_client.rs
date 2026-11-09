#![forbid(unsafe_code)]

//! The Azure DevOps REST subset `internal/authz/providers/azuredevops` needs,
//! ported from `internal/extsvc/azuredevops`:
//!
//! * `NewClient` / `Client.do` (the `api-version=7.0` query parameter, the
//!   `Azure DevOps API HTTP error: code=%d url=%q` shape and the `200–399`
//!   success range),
//! * `GetAuthorizedProfile` and `ListAuthorizedUserOrganizations` against
//!   `https://app.vssps.visualstudio.com/` (the VS App URL; a package-level
//!   mock in the baseline, an injectable override here),
//! * `ListRepositoriesByProjectOrOrg` against the API URL,
//! * `GetOAuthContext`: the `auth.providers` Azure DevOps entry, the
//!   `/.auth/azuredevops/callback` redirect URL derived from `externalURL`,
//!   and the `CustomQueryParams` the baseline sends when refreshing a token
//!   (including the already-escaped `client_assertion`/`assertion` values).
//!
//! `oauthutil.DoRequest`'s refresh semantics are reproduced on the bearer
//! path: a pre-emptive refresh when the token is within its buffer, then one
//! refresh-and-retry on a 401 (the original 401 is returned if the refresh
//! fails).
//!
//! Not ported: the self-imposed/instrumented rate limiters, the 429 retry
//! loop, tracing and the pull-request/project methods the authz provider never
//! calls.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use sg_config::SiteConfiguration;

use super::authz_github_user::{OAuthBearerToken, OAuthContext};

/// `azuredevops.AzureDevOpsAPIURL`.
pub const AZURE_DEVOPS_API_URL: &str = "https://dev.azure.com/";
/// `azuredevops.VisualStudioAppURL`.
pub const VISUAL_STUDIO_APP_URL: &str = "https://app.vssps.visualstudio.com/";
/// `azuredevops.ClientAssertionType`.
pub const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
/// `azuredevops.apiVersion`.
pub const API_VERSION: &str = "7.0";

/// `azuredevops.Profile`, reduced to the fields the authz provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Profile {
    #[serde(default)]
    pub id: String,
    #[serde(default, rename = "displayName")]
    pub display_name: String,
    #[serde(default, rename = "emailAddress")]
    pub email_address: String,
    #[serde(default, rename = "timestamp")]
    pub last_changed: Option<String>,
    #[serde(default, rename = "publicAlias")]
    pub public_alias: String,
}

/// `azuredevops.Org`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Org {
    #[serde(default, rename = "accountId")]
    pub id: String,
    #[serde(default, rename = "accountUri")]
    pub uri: String,
    #[serde(default, rename = "accountName")]
    pub name: String,
}

/// `azuredevops.ListAuthorizedUserOrgsResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ListAuthorizedUserOrgsResponse {
    #[serde(default)]
    pub count: i64,
    #[serde(default)]
    pub value: Vec<Org>,
}

/// `azuredevops.Repository`, reduced to the field `FetchUserPerms` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Repository {
    #[serde(default)]
    pub id: String,
}

/// `azuredevops.ListRepositoriesResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ListRepositoriesResponse {
    #[serde(default)]
    pub value: Vec<Repository>,
    #[serde(default)]
    pub count: i64,
}

/// `azuredevops.HTTPError` plus the client's plain errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureDevOpsError {
    pub message: String,
    pub status: Option<u16>,
}

impl AzureDevOpsError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
        }
    }

    /// `HTTPError.Unauthorized()`.
    pub fn unauthorized(&self) -> bool {
        self.status == Some(401)
    }

    /// `HTTPError.NotFound()`.
    pub fn not_found(&self) -> bool {
        self.status == Some(404)
    }
}

/// The authenticators `AzureDevOpsRestClient` supports (`auth.BasicAuth` for
/// connection validation, `auth.OAuthBearerToken` for user permissions).
pub enum AzureDevOpsAuth {
    Basic { username: String, password: String },
    Bearer(Arc<OAuthBearerToken>),
}

/// `azuredevops.NewClient`.
pub struct AzureDevOpsRestClient {
    http: reqwest::Client,
    api_url: reqwest::Url,
    visual_studio_app_url: reqwest::Url,
    /// `MockVisualStudioAppURL != ""`: the baseline skips the
    /// "Azure DevOps Services only" check when the mock is set.
    visual_studio_app_url_overridden: bool,
    auth: AzureDevOpsAuth,
}

impl AzureDevOpsRestClient {
    pub fn new(
        api_url: &str,
        auth: AzureDevOpsAuth,
        http: reqwest::Client,
    ) -> Result<Self, String> {
        let api_url = reqwest::Url::parse(api_url).map_err(|err| err.to_string())?;
        let visual_studio_app_url =
            reqwest::Url::parse(VISUAL_STUDIO_APP_URL).expect("valid VisualStudioAppURL constant");
        Ok(Self {
            http,
            api_url,
            visual_studio_app_url,
            visual_studio_app_url_overridden: false,
            auth,
        })
    }

    /// `azuredevops.MockVisualStudioAppURL`: only tests set it.
    pub fn with_visual_studio_app_url(mut self, url: Option<reqwest::Url>) -> Self {
        if let Some(url) = url {
            self.visual_studio_app_url = url;
            self.visual_studio_app_url_overridden = true;
        }
        self
    }

    /// `Client.IsAzureDevOpsServices`.
    pub fn is_azure_devops_services(&self) -> bool {
        self.api_url.as_str() == AZURE_DEVOPS_API_URL
    }

    /// `Client.GetAuthorizedProfile`.
    pub async fn get_authorized_profile(&self) -> Result<Profile, AzureDevOpsError> {
        self.do_json(
            "/_apis/profile/profiles/me",
            Some(&self.visual_studio_app_url),
            &[],
        )
        .await
    }

    /// `Client.ListAuthorizedUserOrganizations`.
    pub async fn list_authorized_user_organizations(
        &self,
        profile: &Profile,
    ) -> Result<Vec<Org>, AzureDevOpsError> {
        if !self.visual_studio_app_url_overridden && !self.is_azure_devops_services() {
            return Err(AzureDevOpsError::new(
                "ListAuthorizedUserOrganizations can only be used with Azure DevOps Services",
            ));
        }

        let response: ListAuthorizedUserOrgsResponse = self
            .do_json(
                "_apis/accounts",
                Some(&self.visual_studio_app_url),
                &[("memberId", profile.public_alias.as_str())],
            )
            .await?;
        Ok(response.value)
    }

    /// `Client.ListRepositoriesByProjectOrOrg` for an org or `org/project`.
    pub async fn list_repositories_by_project_or_org(
        &self,
        project_or_org_name: &str,
    ) -> Result<Vec<Repository>, AzureDevOpsError> {
        let path = format!("{project_or_org_name}/_apis/git/repositories");
        let response: ListRepositoriesResponse = self.do_json(&path, None, &[]).await?;
        Ok(response.value)
    }

    /// `Client.do`'s request half: resolve against the API URL (or the VS App
    /// URL override), add `api-version`, authenticate and decode JSON.
    async fn do_json<T: DeserializeOwned>(
        &self,
        path: &str,
        base: Option<&reqwest::Url>,
        query: &[(&str, &str)],
    ) -> Result<T, AzureDevOpsError> {
        let base = base.unwrap_or(&self.api_url);
        let url = resolve_url(base, path, query).map_err(AzureDevOpsError::new)?;

        let response = self.send(&url).await?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| AzureDevOpsError::new(err.to_string()))?;

        if !(200..400).contains(&status.as_u16()) {
            return Err(AzureDevOpsError {
                message: format!(
                    "Azure DevOps API HTTP error: code={} url={:?}",
                    status.as_u16(),
                    url.as_str()
                ),
                status: Some(status.as_u16()),
            });
        }

        serde_json::from_str(&body).map_err(|err| AzureDevOpsError::new(err.to_string()))
    }

    /// `Client.do`'s `oauthutil.DoRequest` half.
    async fn send(&self, url: &reqwest::Url) -> Result<reqwest::Response, AzureDevOpsError> {
        match &self.auth {
            AzureDevOpsAuth::Basic { username, password } => self
                .http
                .get(url.clone())
                .basic_auth(username, Some(password))
                .send()
                .await
                .map_err(|err| AzureDevOpsError::new(err.to_string())),
            AzureDevOpsAuth::Bearer(token) => {
                // `DoRequest`: refresh before the request when the token is
                // within the buffer; a failure is logged and ignored.
                if token.needs_refresh() {
                    let _ = token.refresh().await;
                }
                let response = self.send_bearer(url, token).await?;
                if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                    // One refresh-and-retry; on refresh failure the original
                    // 401 response is returned unchanged.
                    if token.refresh().await.is_ok() {
                        return self.send_bearer(url, token).await;
                    }
                }
                Ok(response)
            }
        }
    }

    async fn send_bearer(
        &self,
        url: &reqwest::Url,
        token: &OAuthBearerToken,
    ) -> Result<reqwest::Response, AzureDevOpsError> {
        let access_token = token.token().access_token;
        self.http
            .get(url.clone())
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {access_token}"),
            )
            .send()
            .await
            .map_err(|err| AzureDevOpsError::new(err.to_string()))
    }
}

/// `url.URL.ResolveReference` plus `url.Values.Encode`: resolve the relative
/// path, then set the sorted query (`api-version` first by key order).
pub(crate) fn resolve_url(
    base: &reqwest::Url,
    path: &str,
    query: &[(&str, &str)],
) -> Result<reqwest::Url, String> {
    let mut url = base.join(path).map_err(|err| err.to_string())?;
    let mut params: BTreeMap<&str, &str> = BTreeMap::new();
    params.insert("api-version", API_VERSION);
    for (key, value) in query {
        params.insert(key, value);
    }
    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish();
    url.set_query(Some(&encoded));
    Ok(url)
}

/// One `auth.providers` entry of type `azureDevOps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureDevOpsOAuthProvider {
    pub client_id: String,
    pub client_secret: String,
}

/// `conf.SiteConfig().AuthProviders`, filtered to Azure DevOps entries.
pub fn azuredevops_oauth_providers(site: &SiteConfiguration) -> Vec<AzureDevOpsOAuthProvider> {
    let Some(entries) = site.raw().get("auth.providers").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|entry| entry.get("type").and_then(|v| v.as_str()) == Some("azureDevOps"))
        .map(|entry| AzureDevOpsOAuthProvider {
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

/// `azuredevops.GetOAuthContext(refreshToken)`: the first Azure DevOps authn
/// provider, the VS App token endpoint and the custom refresh-token body
/// parameters. `external_url` is the site `externalURL` used by
/// `GetRedirectURL`.
pub fn get_oauth_context(
    providers: &[AzureDevOpsOAuthProvider],
    external_url: &str,
    refresh_token: &str,
) -> Result<OAuthContext, String> {
    let Some(provider) = providers.first() else {
        return Err(
            "No authprovider configured for AzureDevOps, check site configuration.".to_owned(),
        );
    };

    // `url.JoinPath(VisualStudioAppURL, "oauth2/token")`.
    let token_url = format!("{VISUAL_STUDIO_APP_URL}oauth2/token");

    // `GetRedirectURL(nil)`: parse `externalURL` and replace its path.
    let mut redirect_url = reqwest::Url::parse(external_url).map_err(|_| {
        "Could not parse `externalURL`, which is needed to determine the OAuth callback URL."
            .to_owned()
    })?;
    redirect_url.set_path("/.auth/azuredevops/callback");

    // The Azure API expects these custom values in the refresh POST body.
    // `client_assertion` and `assertion` are passed through `url.QueryEscape`
    // before `url.Values.Encode` escapes them a second time, exactly like the
    // baseline.
    let mut custom_query_params = BTreeMap::new();
    custom_query_params.insert(
        "client_assertion_type".to_owned(),
        CLIENT_ASSERTION_TYPE.to_owned(),
    );
    custom_query_params.insert(
        "client_assertion".to_owned(),
        go_query_escape(&provider.client_secret),
    );
    custom_query_params.insert("grant_type".to_owned(), "refresh_token".to_owned());
    custom_query_params.insert("assertion".to_owned(), go_query_escape(refresh_token));
    custom_query_params.insert("redirect_uri".to_owned(), redirect_url.to_string());

    Ok(OAuthContext {
        client_id: provider.client_id.clone(),
        client_secret: provider.client_secret.clone(),
        token_url,
        custom_query_params,
    })
}

/// `url.QueryEscape`: keep `A-Za-z0-9-_.~`, encode a space as `+` and every
/// other byte as `%XX` with uppercase hex.
pub fn go_query_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(byte as char);
            }
            b' ' => escaped.push('+'),
            _ => {
                escaped.push('%');
                escaped.push_str(&format!("{byte:02X}"));
            }
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_url_adds_sorted_api_version() {
        let base = reqwest::Url::parse("https://dev.azure.com/").expect("URL");
        let url = resolve_url(&base, "org/_apis/git/repositories", &[]).expect("resolve");
        assert_eq!(
            url.as_str(),
            "https://dev.azure.com/org/_apis/git/repositories?api-version=7.0"
        );

        let base = reqwest::Url::parse("https://app.vssps.visualstudio.com/").expect("URL");
        let url =
            resolve_url(&base, "_apis/accounts", &[("memberId", "alias-1")]).expect("resolve");
        assert_eq!(
            url.as_str(),
            "https://app.vssps.visualstudio.com/_apis/accounts?api-version=7.0&memberId=alias-1"
        );
    }

    #[test]
    fn absolute_path_replaces_the_base_path() {
        let base = reqwest::Url::parse("https://dev.azure.com/base/").expect("URL");
        let url = resolve_url(&base, "/_apis/profile/profiles/me", &[]).expect("resolve");
        assert_eq!(
            url.as_str(),
            "https://dev.azure.com/_apis/profile/profiles/me?api-version=7.0"
        );
    }

    #[test]
    fn go_query_escape_matches_the_baseline() {
        assert_eq!(go_query_escape("abc-_.~"), "abc-_.~");
        assert_eq!(go_query_escape("a b"), "a+b");
        assert_eq!(go_query_escape("s:1/2"), "s%3A1%2F2");
        assert_eq!(go_query_escape("é"), "%C3%A9");
    }

    #[test]
    fn oauth_context_carries_the_custom_refresh_params() {
        let providers = vec![AzureDevOpsOAuthProvider {
            client_id: "id".to_owned(),
            client_secret: "strong secret".to_owned(),
        }];
        let oauth = get_oauth_context(
            &providers,
            "https://sourcegraph.example.com",
            "refresh:token",
        )
        .expect("oauth context");
        assert_eq!(oauth.client_id, "id");
        assert_eq!(
            oauth.token_url,
            "https://app.vssps.visualstudio.com/oauth2/token"
        );
        assert_eq!(
            oauth
                .custom_query_params
                .get("client_assertion_type")
                .map(String::as_str),
            Some(CLIENT_ASSERTION_TYPE)
        );
        // Pre-escaped, then escaped again by the form encoder.
        assert_eq!(
            oauth
                .custom_query_params
                .get("client_assertion")
                .map(String::as_str),
            Some("strong+secret")
        );
        assert_eq!(
            oauth
                .custom_query_params
                .get("assertion")
                .map(String::as_str),
            Some("refresh%3Atoken")
        );
        assert_eq!(
            oauth
                .custom_query_params
                .get("redirect_uri")
                .map(String::as_str),
            Some("https://sourcegraph.example.com/.auth/azuredevops/callback")
        );
    }

    #[test]
    fn missing_auth_provider_is_an_error() {
        assert_eq!(
            get_oauth_context(&[], "https://sourcegraph.example.com", "r").unwrap_err(),
            "No authprovider configured for AzureDevOps, check site configuration."
        );
    }
}
