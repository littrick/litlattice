#![forbid(unsafe_code)]

//! `Query.clientConfiguration` (`cmd/frontend/graphqlbackend/client_configuration.go:36-95`).
//!
//! Lists the code-host external services of the four kinds whose instances
//! the browser/editor content scripts can be injected into, decrypts each
//! configuration (`encryption_key_id` -> keyring `externalServiceKey`),
//! parses it as JSONC and collects every `url` as a set. The parent
//! Sourcegraph URL comes from the site configuration
//! (`parentSourcegraph.url`).
//!
//! The baseline iterates a Go map, so the order of `contentScriptUrls` is
//! unspecified; the Rust port returns the sorted set (see the overview §3.1)
//! and the differential harness sorts both sides before comparing.
//!
//! Configurations that are not an object, or whose `url` is neither a string
//! nor `null`/absent, fail like `json.Unmarshal` does in the baseline, but the
//! exact Go struct-field message is not reproduced (overview §3.2).

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use sg_encryption::Key;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sg_jsonx::{JsonValue, ParseOptions};

/// `extsvc.KindGitHub`.
pub const KIND_GITHUB: &str = "GITHUB";
/// `extsvc.KindBitbucketServer` (`BITBUCKETSERVER`, not `BITBUCKET_SERVER`).
pub const KIND_BITBUCKET_SERVER: &str = "BITBUCKETSERVER";
/// `extsvc.KindGitLab`.
pub const KIND_GITLAB: &str = "GITLAB";
/// `extsvc.KindPhabricator`.
pub const KIND_PHABRICATOR: &str = "PHABRICATOR";

/// `ClientConfiguration`'s `Kinds` filter (`client_configuration.go:37-44`).
pub const KINDS: [&str; 4] = [
    KIND_GITHUB,
    KIND_BITBUCKET_SERVER,
    KIND_GITLAB,
    KIND_PHABRICATOR,
];

/// The `external_services` columns `ClientConfiguration` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalServiceConfig {
    pub kind: String,
    /// `external_services.config`, still encrypted.
    pub config: String,
    /// `external_services.encryption_key_id`; `""` when unencrypted.
    pub encryption_key_id: String,
}

/// `db.ExternalServices().List(ctx, ExternalServicesListOptions{Kinds: ...})`.
#[async_trait]
pub trait ExternalServiceConfigSource: Send + Sync {
    async fn list_by_kinds(&self, kinds: &[&str]) -> Result<Vec<ExternalServiceConfig>, String>;
}

/// Production source: the `external_services` table.
pub struct PgExternalServiceConfigSource {
    store: sg_db::external_services::ExternalServiceStore,
}

impl PgExternalServiceConfigSource {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            store: sg_db::external_services::ExternalServiceStore::new(pool),
        }
    }
}

#[async_trait]
impl ExternalServiceConfigSource for PgExternalServiceConfigSource {
    async fn list_by_kinds(&self, kinds: &[&str]) -> Result<Vec<ExternalServiceConfig>, String> {
        self.store
            .list_by_kinds(kinds)
            .await
            .map(|services| {
                services
                    .into_iter()
                    .map(|service| ExternalServiceConfig {
                        kind: service.kind,
                        config: service.config,
                        encryption_key_id: service.encryption_key_id,
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }
}

/// `clientConfigurationResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfigurationDetails {
    /// `contentScriptUrls` (sorted; the baseline order is a map iteration).
    pub content_script_urls: Vec<String>,
    /// `parentSourcegraph.url`.
    pub parent_sourcegraph_url: String,
}

/// `schemaResolver.ClientConfiguration` (`:36-95`).
pub async fn client_configuration(
    source: &dyn ExternalServiceConfigSource,
    key: Option<Arc<dyn Key>>,
    parent_sourcegraph_url: String,
) -> Result<ClientConfigurationDetails, String> {
    let services = source.list_by_kinds(&KINDS).await?;

    let mut url_map = BTreeSet::new();
    for service in services {
        // `service.Config.Decrypt(ctx)`.
        let raw_config = sg_encryption::maybe_decrypt(
            key.as_deref(),
            &service.config,
            &service.encryption_key_id,
        )
        .await
        .map_err(|error| error.to_string())?;
        // The `switch service.Kind` leaves `url` empty for unknown kinds and
        // skips `jsonc.Unmarshal` entirely.
        let url = match service.kind.as_str() {
            KIND_GITHUB | KIND_BITBUCKET_SERVER | KIND_GITLAB | KIND_PHABRICATOR => {
                config_url(&raw_config)?
            }
            _ => String::new(),
        };
        url_map.insert(url);
    }

    Ok(ClientConfigurationDetails {
        content_script_urls: url_map.into_iter().collect(),
        parent_sourcegraph_url,
    })
}

/// `jsonc.Unmarshal(rawConfig, &schema.XConnection{})` followed by `url`.
fn config_url(raw_config: &str) -> Result<String, String> {
    let (value, errors) = sg_jsonx::parse(
        raw_config,
        ParseOptions {
            comments: true,
            trailing_commas: true,
        },
    );
    if !errors.is_empty() {
        // `jsonc.Parse`: `failed to parse JSON: %v` over the error-code slice.
        let names: Vec<&str> = errors.iter().map(|error| error.code_name()).collect();
        return Err(format!("failed to parse JSON: [{}]", names.join(" ")));
    }
    match value {
        // `json.Unmarshal("null", &struct)` succeeds with the zero value.
        None | Some(JsonValue::Null) => Ok(String::new()),
        Some(JsonValue::Object(mut map)) => match map.remove("url") {
            // `url` absent or `null` leaves the zero value.
            None | Some(JsonValue::Null) => Ok(String::new()),
            Some(JsonValue::String(url)) => Ok(url),
            Some(other) => Err(format!(
                "json: cannot unmarshal {} into Go struct field .url of type string",
                json_type_name(&other)
            )),
        },
        Some(other) => Err(format!(
            "json: cannot unmarshal {} into Go value of type struct",
            json_type_name(&other)
        )),
    }
}

/// The Go type names used by `encoding/json` errors.
fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// `conf.Get().ParentSourcegraph.Url` (`schema/schema.go:3358-3359`).
pub fn parent_sourcegraph_url(site: &sg_config::SiteConfiguration) -> String {
    site.raw()
        .get("parentSourcegraph")
        .and_then(|parent| parent.get("url"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `parentSourcegraphResolver`.
struct ParentSourcegraphObject {
    url: String,
}

impl ObjectValue for ParentSourcegraphObject {
    fn type_name(&self) -> &'static str {
        "ParentSourcegraphDetails"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.url.clone())))),
            _ => Ok(None),
        }
    }
}

/// `clientConfigurationResolver`.
pub struct ClientConfigurationObject {
    details: ClientConfigurationDetails,
}

impl ClientConfigurationObject {
    pub fn new(details: ClientConfigurationDetails) -> Self {
        Self { details }
    }
}

impl ObjectValue for ClientConfigurationObject {
    fn type_name(&self) -> &'static str {
        "ClientConfigurationDetails"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "contentScriptUrls" => Ok(Some(FieldValue::List {
                items: self
                    .details
                    .content_script_urls
                    .iter()
                    .map(|url| FieldValue::Json(Json::Str(url.clone())))
                    .collect(),
                element_type: Some("String"),
            })),
            "parentSourcegraph" => Ok(Some(FieldValue::Object(Arc::new(
                ParentSourcegraphObject {
                    url: self.details.parent_sourcegraph_url.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

/// The prefetched `Query.clientConfiguration` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientConfigurationLookup {
    Found(ClientConfigurationDetails),
    Error(String),
}

/// Prefetch `Query.clientConfiguration` when the request selects it.
pub async fn prefetch_client_configuration(
    source: &dyn ExternalServiceConfigSource,
    key: Option<Arc<dyn Key>>,
    parent_sourcegraph_url: String,
    fields: &[RootField],
) -> Option<ClientConfigurationLookup> {
    if !fields
        .iter()
        .any(|field| field.name == "clientConfiguration")
    {
        return None;
    }
    match client_configuration(source, key, parent_sourcegraph_url).await {
        Ok(details) => Some(ClientConfigurationLookup::Found(details)),
        Err(message) => Some(ClientConfigurationLookup::Error(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeSource {
        services: Vec<ExternalServiceConfig>,
        kinds: Mutex<Vec<String>>,
    }

    impl FakeSource {
        fn new(services: Vec<ExternalServiceConfig>) -> Self {
            Self {
                services,
                kinds: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ExternalServiceConfigSource for FakeSource {
        async fn list_by_kinds(
            &self,
            kinds: &[&str],
        ) -> Result<Vec<ExternalServiceConfig>, String> {
            *self.kinds.lock().unwrap() = kinds.iter().map(|kind| (*kind).to_owned()).collect();
            Ok(self.services.clone())
        }
    }

    fn service(kind: &str, config: &str) -> ExternalServiceConfig {
        ExternalServiceConfig {
            kind: kind.to_owned(),
            config: config.to_owned(),
            encryption_key_id: String::new(),
        }
    }

    #[tokio::test]
    async fn collects_sorted_unique_urls_from_every_kind() {
        let source = FakeSource::new(vec![
            service(KIND_GITHUB, r#"{"url":"https://github.example.com"}"#),
            service(
                KIND_BITBUCKET_SERVER,
                r#"{"url":"https://bitbucket.example.com"}"#,
            ),
            service(
                KIND_GITLAB,
                r#"{ // comment
                "url": "https://gitlab.example.com",
            }"#,
            ),
            service(KIND_PHABRICATOR, r#"{"url":"https://github.example.com"}"#),
        ]);
        let details = client_configuration(&source, None, String::new())
            .await
            .expect("configuration");
        assert_eq!(
            details.content_script_urls,
            vec![
                "https://bitbucket.example.com",
                "https://github.example.com",
                "https://gitlab.example.com",
            ]
        );
        assert_eq!(
            *source.kinds.lock().unwrap(),
            vec!["GITHUB", "BITBUCKETSERVER", "GITLAB", "PHABRICATOR"]
        );
    }

    #[tokio::test]
    async fn missing_url_is_the_empty_string() {
        let source = FakeSource::new(vec![
            service(KIND_GITHUB, r#"{"token":"x"}"#),
            service(KIND_GITLAB, r#"{"url":null}"#),
            service(KIND_PHABRICATOR, "null"),
        ]);
        let details = client_configuration(&source, None, String::new())
            .await
            .expect("configuration");
        assert_eq!(details.content_script_urls, vec![String::new()]);
    }

    #[tokio::test]
    async fn encrypted_configs_are_decrypted_with_the_external_service_key() {
        // `noop` leaves the plaintext unchanged, like the baseline key.
        let key: Arc<dyn Key> = Arc::new(sg_encryption::NoopKey::default());
        let key_id = key.version().await.expect("version").json();
        let source = FakeSource::new(vec![ExternalServiceConfig {
            kind: KIND_GITHUB.to_owned(),
            config: r#"{"url":"https://encrypted.example.com"}"#.to_owned(),
            encryption_key_id: key_id,
        }]);
        let details = client_configuration(&source, Some(key), String::new())
            .await
            .expect("configuration");
        assert_eq!(
            details.content_script_urls,
            vec!["https://encrypted.example.com"]
        );
    }

    #[tokio::test]
    async fn encrypted_config_without_a_key_is_an_error() {
        let source = FakeSource::new(vec![ExternalServiceConfig {
            kind: KIND_GITHUB.to_owned(),
            config: r#"{"url":"https://encrypted.example.com"}"#.to_owned(),
            encryption_key_id: r#"{"type":"noop","name":"noop","version":""}"#.to_owned(),
        }]);
        let error = client_configuration(&source, None, String::new())
            .await
            .unwrap_err();
        assert_eq!(
            error,
            "key mismatch: value is encrypted but no encryption key available in site-config"
        );
    }

    #[tokio::test]
    async fn parse_errors_use_the_baseline_text() {
        let source = FakeSource::new(vec![service(KIND_GITHUB, r#"{"url": }"#)]);
        let error = client_configuration(&source, None, String::new())
            .await
            .unwrap_err();
        assert_eq!(error, "failed to parse JSON: [ValueExpected]");
    }

    #[tokio::test]
    async fn non_string_url_is_rejected() {
        let source = FakeSource::new(vec![service(KIND_GITHUB, r#"{"url":5}"#)]);
        let error = client_configuration(&source, None, String::new())
            .await
            .unwrap_err();
        assert!(error.contains("cannot unmarshal number"), "{error}");
    }

    #[tokio::test]
    async fn unknown_kinds_are_never_parsed() {
        // The database filters kinds, but the baseline switch would leave the
        // URL empty without touching the config.
        let source = FakeSource::new(vec![service("GITEA", "not json")]);
        let details = client_configuration(&source, None, String::new())
            .await
            .expect("configuration");
        assert_eq!(details.content_script_urls, vec![String::new()]);
    }

    #[test]
    fn parent_sourcegraph_url_reads_the_site_config() {
        let site = sg_config::SiteConfiguration::parse(
            r#"{"parentSourcegraph":{"url":"https://sourcegraph.example.com"}}"#,
        )
        .expect("parse");
        assert_eq!(
            parent_sourcegraph_url(&site),
            "https://sourcegraph.example.com"
        );
        let site = sg_config::SiteConfiguration::parse("{}").expect("parse");
        assert_eq!(parent_sourcegraph_url(&site), "");
    }
}
