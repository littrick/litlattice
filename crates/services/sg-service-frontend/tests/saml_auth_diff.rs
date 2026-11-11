//! M9.8jt differential harness for `saml_auth` against the Go oracle
//! (`cmd/frontend/internal/auth/saml/zz_m9jt_oracle_test.go`).
//!
//! Set `SG_SAML_AUTH_HARNESS=1`, `SAML_AUTH_CORPUS=<corpus.json>` and
//! `SAML_AUTH_OUT=<dir>`; for every corpus case this writes `<name>.txt` in the
//! same format as the oracle.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_config::SiteConfiguration;
use sg_service_frontend::oauth_login::{go_parse_url, parse_query, query_get};
use sg_service_frontend::saml_auth::{
    allow_signin, decode_authn_request, get_service_provider, read_authn_response, saml_middleware,
    with_config_defaults, RelayState, SamlDeps, SamlHttpClient, SamlHttpRequest,
    SamlMetadataResponse, SamlNewUser, SamlProviderConfig, SamlProviderSet, SamlProvisioner,
    SamlUser, SamlUserStore, SignInProviderInfo,
};
use sg_service_frontend::user_provisioning::{
    ProvisionError, ProvisionOutcome, UsernameSuffixSource,
};
use sg_session::{SessionIdSource, SessionStore, SessionStoreError};

struct MemStore(Mutex<BTreeMap<String, Vec<u8>>>);

#[async_trait]
impl SessionStore for MemStore {
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        Ok(self.0.lock().unwrap().get(id).cloned())
    }

    async fn save(
        &self,
        id: &str,
        payload: Vec<u8>,
        _ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        self.0.lock().unwrap().insert(id.to_owned(), payload);
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
        self.0.lock().unwrap().remove(id);
        Ok(())
    }
}

struct FixedIds;

impl SessionIdSource for FixedIds {
    fn generate(&self) -> String {
        "m9jt-session-id".to_owned()
    }
}

#[derive(Default, Clone)]
struct RecordedProvision {
    username: String,
    email: String,
    display_name: String,
    service_id: String,
    client_id: String,
    account_id: String,
    create_if_not_exist: bool,
}

struct FakeProvisioner {
    mode: String,
    user_id: i32,
    safe_message: String,
    recorded: Mutex<Option<RecordedProvision>>,
}

#[async_trait]
impl SamlProvisioner for FakeProvisioner {
    async fn get_or_create_user(
        &self,
        user: &SamlNewUser<'_>,
    ) -> Result<ProvisionOutcome, ProvisionError> {
        *self.recorded.lock().unwrap() = Some(RecordedProvision {
            username: user.username.to_owned(),
            email: user.email.to_owned(),
            display_name: user.display_name.to_owned(),
            service_id: user.service_id.to_owned(),
            client_id: user.client_id.to_owned(),
            account_id: user.account_id.to_owned(),
            create_if_not_exist: user.create_if_not_exist,
        });
        match self.mode.as_str() {
            "created" => Ok(ProvisionOutcome::Created {
                user_id: self.user_id,
            }),
            "error" => Err(ProvisionError::new(
                self.safe_message.clone(),
                "provision failed",
            )),
            _ => Ok(ProvisionOutcome::Existing {
                user_id: self.user_id,
            }),
        }
    }
}

struct FixedUserStore {
    user_id: i32,
    created_at: OffsetDateTime,
}

#[async_trait]
impl SamlUserStore for FixedUserStore {
    async fn get_by_id(&self, _id: i32) -> Result<Option<SamlUser>, String> {
        Ok(Some(SamlUser {
            id: self.user_id,
            created_at: self.created_at,
            site_admin: false,
        }))
    }
}

struct NoSuffix;

impl UsernameSuffixSource for NoSuffix {
    fn add_random_suffix(&self, username: &str) -> Result<String, String> {
        Ok(format!("{username}-suffix"))
    }
}

struct FakeHttp {
    status: u16,
    body: Vec<u8>,
    error: bool,
}

#[async_trait]
impl SamlHttpClient for FakeHttp {
    async fn fetch(&self, _url: &str) -> Result<SamlMetadataResponse, String> {
        if self.error {
            return Err("connection refused".to_owned());
        }
        Ok(SamlMetadataResponse {
            status: self.status,
            body: self.body.clone(),
        })
    }
}

fn s(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn esc(value: &str) -> String {
    value.replace('\r', "\\r").replace('\n', "\\n")
}

fn obj(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("valid RFC3339 time")
}

fn config_for(provider: &Value, external_url: &str) -> SamlProviderConfig {
    with_config_defaults(&obj(provider), external_url)
}

fn http_for(case: &Value) -> FakeHttp {
    let http = case.get("http").cloned().unwrap_or(Value::Null);
    FakeHttp {
        status: http.get("status").and_then(Value::as_u64).unwrap_or(500) as u16,
        body: s(&http, "body").into_bytes(),
        error: http.get("error").and_then(Value::as_bool).unwrap_or(false),
    }
}

fn site_with(external_url: &str, providers: Vec<Value>) -> SiteConfiguration {
    let site = json!({
        "externalURL": external_url,
        "auth.providers": providers,
    });
    SiteConfiguration::parse(&site.to_string()).expect("valid site config")
}

fn sp_error(text: &str) -> String {
    if text.starts_with("fetching SAML Identity Provider metadata") {
        return "fetching SAML Identity Provider metadata".to_owned();
    }
    if text.starts_with("parsing SAML Identity Provider metadata") {
        return "parsing SAML Identity Provider metadata".to_owned();
    }
    text.to_owned()
}

fn normalize_location(location: &str) -> String {
    let Some(parsed) = go_parse_url(location) else {
        return location.to_owned();
    };
    if parsed.host != "idp.example.com" || parsed.path != "/sso" {
        return location.to_owned();
    }
    let query = parse_query(&parsed.raw_query);
    format!(
        "<idp-login>?SAMLRequest={}&RelayState={}&SigAlg={}&Signature={}",
        !query_get(&query, "SAMLRequest").is_empty(),
        query_get(&query, "RelayState"),
        query_get(&query, "SigAlg"),
        !query_get(&query, "Signature").is_empty()
    )
}

fn normalize_body(status: u16, body: &str) -> String {
    if status == 302 && body.starts_with("<a href=\"") {
        return "<redirect-link>".to_owned();
    }
    body.to_owned()
}

fn cookie_line(cookie: &sg_session::SetCookie) -> String {
    let present = if cookie.value.is_empty() {
        "empty"
    } else {
        "present"
    };
    // The test filesystem session store (Go) and the production-style Rust
    // session layer differ in HttpOnly/SameSite; those are not SAML decisions.
    format!(
        "{}|{}|{}|{}|{}",
        cookie.name, cookie.path, cookie.max_age, cookie.secure, present
    )
}

fn describe(response: &sg_service_frontend::saml_auth::SamlHttpResponse) -> Vec<String> {
    let mut lines = vec![
        format!("status: {}", response.status),
        format!("location: {}", esc(&response.location)),
        format!(
            "body: {}",
            esc(&normalize_body(
                response.status,
                &String::from_utf8_lossy(&response.body)
            ))
        ),
    ];
    let mut cookies: Vec<String> = response.set_cookies.iter().map(cookie_line).collect();
    cookies.sort();
    lines.push(format!("cookies: {}", cookies.len()));
    for (index, cookie) in cookies.iter().enumerate() {
        lines.push(format!("cookie.{index}: {cookie}"));
    }
    lines
}

fn provision_lines(recorded: &RecordedProvision) -> Vec<String> {
    vec![
        format!(
            "provision: saml;{};{};{}",
            recorded.service_id, recorded.client_id, recorded.account_id
        ),
        format!("provision.username: {}", recorded.username),
        format!("provision.email: {}", recorded.email),
        "provision.emailVerified: true".to_owned(),
        format!("provision.displayName: {}", recorded.display_name),
        format!(
            "provision.createIfNotExist: {}",
            recorded.create_if_not_exist
        ),
    ]
}

struct Deps<'a> {
    provisioner: &'a FakeProvisioner,
    users: &'a FixedUserStore,
    suffix: &'a NoSuffix,
}

#[allow(clippy::too_many_arguments)]
async fn run_case(
    case: &Value,
    external_url: &str,
    now: OffsetDateTime,
    store: &MemStore,
    ids: &FixedIds,
    deps_extra: &Deps<'_>,
) -> Vec<String> {
    let kind = s(case, "kind");
    match kind.as_str() {
        "config_validate" => {
            let site_value = case.get("site").cloned().unwrap_or(Value::Null);
            let site = SiteConfiguration::parse(&site_value.to_string())
                .expect("valid validation site config");
            let problems = sg_service_frontend::saml_auth::validate_config(&site);
            let mut lines = vec![format!("problems: {}", problems.len())];
            for (index, problem) in problems.iter().enumerate() {
                lines.push(format!("problem.{index}: {}", esc(problem)));
            }
            lines
        }
        "config_defaults" => {
            let config = config_for(case.get("provider").unwrap(), &s(case, "externalURL"));
            vec![
                format!("displayName: {}", config.display_name),
                format!("serviceProviderIssuer: {}", config.service_provider_issuer),
                format!("nameIDFormat: {}", config.name_id_format()),
                format!("configID.multiple: {}", config.provider_config_id(true)),
                format!("configID.single: {}", config.provider_config_id(false)),
                format!("canonicalJSON: {}", esc(&config.canonical_json)),
            ]
        }
        "relay" => {
            let relay = RelayState {
                provider_id: s(case, "providerID"),
                return_to_url: s(case, "returnToURL"),
            };
            let encoded = relay.encode();
            let decoded = RelayState::decode(&encoded);
            vec![
                format!("encode: {encoded}"),
                format!("decode.providerID: {}", decoded.provider_id),
                format!("decode.returnToURL: {}", decoded.return_to_url),
            ]
        }
        "relay_decode" => {
            let decoded = RelayState::decode(&s(case, "encoded"));
            vec![
                format!("providerID: {}", decoded.provider_id),
                format!("returnToURL: {}", decoded.return_to_url),
            ]
        }
        "allow_signin" => {
            let provider = json!({
                "type": "saml",
                "allowGroups": case.get("allowGroups").cloned().unwrap_or(Value::Null),
            });
            let config = config_for(&provider, "");
            let mut groups = BTreeMap::new();
            if let Some(items) = case.get("groups").and_then(Value::as_array) {
                for item in items {
                    if let Some(group) = item.as_str() {
                        groups.insert(group.to_owned(), true);
                    }
                }
            }
            vec![format!("allow: {}", allow_signin(&config, &groups))]
        }
        "sp" => {
            let external = s(case, "externalURL");
            let config = config_for(case.get("provider").unwrap(), &external);
            let http = http_for(case);
            match get_service_provider(&config, &http, &external).await {
                Ok(sp) => vec![
                    format!("ssoURL: {}", sp.identity_provider_sso_url),
                    format!("idpIssuer: {}", sp.identity_provider_issuer),
                    format!("spIssuer: {}", sp.service_provider_issuer),
                    format!("acsURL: {}", sp.assertion_consumer_service_url),
                    format!("audience: {}", sp.audience_uri),
                    format!("nameIDFormat: {}", sp.name_id_format),
                    format!("signAuthnRequests: {}", sp.sign_authn_requests),
                    format!("skipSignatureValidation: {}", sp.skip_signature_validation),
                    format!("idpCertCount: {}", sp.idp_certificates.len()),
                ],
                Err(error) => vec![format!("error: {}", esc(&sp_error(&error)))],
            }
        }
        "read_response" => {
            let config = config_for(case.get("provider").unwrap(), external_url);
            let http = FakeHttp {
                status: 500,
                body: Vec::new(),
                error: false,
            };
            let provider = sg_service_frontend::saml_auth::SamlProvider::new(
                config,
                false,
                std::sync::Arc::new(http),
            );
            if let Err(error) = provider.refresh(external_url).await {
                return vec![
                    "class: provider".to_owned(),
                    format!("error: {}", esc(&error)),
                ];
            }
            let encoded = s(case, "encoded");
            let now = parse_time(&s(case, "now"));
            match read_authn_response(&provider, &encoded, now).await {
                Ok(info) => {
                    let groups: Vec<String> = info.groups.keys().cloned().collect();
                    vec![
                        "class: ok".to_owned(),
                        format!("email: {}", info.email),
                        format!("username: {}", info.unnormalized_username),
                        format!("displayName: {}", info.display_name),
                        format!("groups: {}", groups.join(",")),
                        format!("serviceID: {}", info.service_id),
                        format!("clientID: {}", info.client_id),
                        format!("accountID: {}", info.account_id),
                    ]
                }
                Err(text) => {
                    let class = if text.starts_with("reading AuthnResponse assertions") {
                        "retrieve"
                    } else if text.starts_with("invalid SAML AuthnResponse") {
                        "warning"
                    } else if text == "the SAML response did not contain a valid NameID" {
                        "nameid"
                    } else if text == "the SAML response did not contain an email attribute" {
                        "email"
                    } else if text == "the SAML response did not contain a username attribute" {
                        "username"
                    } else {
                        "other"
                    };
                    let mut lines = vec![format!("class: {class}")];
                    if matches!(class, "warning" | "nameid" | "email" | "username") {
                        lines.push(format!("error: {}", esc(&text)));
                    }
                    lines
                }
            }
        }
        "login" => {
            let request = case.get("request").cloned().unwrap_or(Value::Null);
            let path = s(&request, "path");
            let provider = case.get("provider").cloned().unwrap();
            let site = site_with(external_url, vec![provider.clone()]);
            let set = SamlProviderSet::from_site(
                &site,
                std::sync::Arc::new(FakeHttp {
                    status: 500,
                    body: Vec::new(),
                    error: false,
                }),
            );
            let sign_in = vec![SignInProviderInfo {
                provider_type: "saml".to_owned(),
                config_id: String::new(),
                hidden: false,
                no_sign_in: false,
            }];
            let deps = SamlDeps {
                providers: &set,
                sign_in_providers: &sign_in,
                auth_public: false,
                external_url,
                store,
                ids,
                cookie_key: b"m9jt-cookie-key",
                secure: false,
                configured_expiry: None,
                now,
                provisioner: deps_extra.provisioner,
                users: deps_extra.users,
                suffix: deps_extra.suffix,
                sourcegraph_dotcom: false,
                license_expired: false,
            };
            let http_request = SamlHttpRequest {
                method: "GET".to_owned(),
                path: path.clone(),
                url_string: format!("{external_url}{path}"),
                referer: s(&request, "referer"),
                ..SamlHttpRequest::default()
            };
            let response = saml_middleware(&deps, &http_request)
                .await
                .expect("login endpoint responds");
            let parsed = go_parse_url(&response.location).expect("valid redirect");
            let query = parse_query(&parsed.raw_query);
            let mut keys: Vec<String> = query.iter().map(|(key, _)| key.clone()).collect();
            keys.sort();
            keys.dedup();
            let relay = RelayState::decode(&query_get(&query, "RelayState"));
            let authn = decode_authn_request(&query_get(&query, "SAMLRequest"))
                .expect("valid AuthnRequest");
            let config = config_for(&provider, external_url);
            vec![
                format!("status: {}", response.status),
                format!("scheme: {}", parsed.scheme),
                format!("host: {}", parsed.host),
                format!("path: {}", parsed.path),
                format!("queryKeys: {}", keys.join(",")),
                format!("relay.providerID: {}", relay.provider_id),
                format!("relay.returnToURL: {}", relay.return_to_url),
                format!(
                    "relay.matchesConfigID: {}",
                    relay.provider_id == config.provider_config_id(false)
                ),
                format!("sigAlg: {}", query_get(&query, "SigAlg")),
                format!("signature: {}", !query_get(&query, "Signature").is_empty()),
                format!("authn.version: {}", authn.version),
                format!("authn.destination: {}", authn.destination),
                format!("authn.acs: {}", authn.assertion_consumer_service_url),
                format!("authn.binding: {}", authn.protocol_binding),
                format!("authn.issuer: {}", authn.issuer),
                format!("authn.nameIDFormat: {}", authn.name_id_format),
                format!("authn.allowCreate: {}", authn.allow_create),
                format!("authn.signed: {}", authn.signed),
            ]
        }
        "metadata" => {
            let provider = case.get("provider").cloned().unwrap();
            let site = site_with(external_url, vec![provider]);
            let set = SamlProviderSet::from_site(
                &site,
                std::sync::Arc::new(FakeHttp {
                    status: 500,
                    body: Vec::new(),
                    error: false,
                }),
            );
            let sign_in = vec![SignInProviderInfo {
                provider_type: "saml".to_owned(),
                config_id: String::new(),
                hidden: false,
                no_sign_in: false,
            }];
            let deps = SamlDeps {
                providers: &set,
                sign_in_providers: &sign_in,
                auth_public: false,
                external_url,
                store,
                ids,
                cookie_key: b"m9jt-cookie-key",
                secure: false,
                configured_expiry: None,
                now,
                provisioner: deps_extra.provisioner,
                users: deps_extra.users,
                suffix: deps_extra.suffix,
                sourcegraph_dotcom: false,
                license_expired: false,
            };
            let response = saml_middleware(
                &deps,
                &SamlHttpRequest {
                    method: "GET".to_owned(),
                    path: "/.auth/saml/metadata".to_owned(),
                    url_string: format!("{external_url}/.auth/saml/metadata"),
                    ..SamlHttpRequest::default()
                },
            )
            .await
            .expect("metadata endpoint responds");
            let root =
                sg_service_frontend::saml_auth::parse_xml(&response.body).expect("metadata XML");
            let entity_id = root
                .attrs
                .iter()
                .find(|attr| attr.key == "entityID")
                .map(|attr| attr.value.clone())
                .unwrap_or_default();
            let valid_until = root.attrs.iter().any(|attr| attr.key == "validUntil");
            let descriptor_count = root
                .children
                .iter()
                .filter_map(|node| match node {
                    sg_service_frontend::saml_auth::XmlNode::Element(element) => Some(element),
                    _ => None,
                })
                .filter(|element| {
                    element.local == "SPSSODescriptor" || element.local == "IDPSSODescriptor"
                })
                .count();
            let mut lines = vec![
                format!("status: {}", response.status),
                format!("contentType: {}", response.content_type),
                format!("entityID: {entity_id}"),
                format!("validUntil.present: {valid_until}"),
                format!("keyDescriptors: {descriptor_count}"),
            ];
            if let Some(sp) = root
                .children
                .iter()
                .filter_map(|node| match node {
                    sg_service_frontend::saml_auth::XmlNode::Element(element) => Some(element),
                    _ => None,
                })
                .find(|element| element.local == "SPSSODescriptor")
            {
                let attr = |name: &str| {
                    sp.attrs
                        .iter()
                        .find(|attr| attr.key == name)
                        .map(|attr| attr.value.clone())
                        .unwrap_or_default()
                };
                let children: Vec<&sg_service_frontend::saml_auth::XmlElement> = sp
                    .children
                    .iter()
                    .filter_map(|node| match node {
                        sg_service_frontend::saml_auth::XmlNode::Element(element) => Some(element),
                        _ => None,
                    })
                    .collect();
                let key_descriptor_count = children
                    .iter()
                    .filter(|element| element.local == "KeyDescriptor")
                    .count();
                let services: Vec<&sg_service_frontend::saml_auth::XmlElement> = children
                    .iter()
                    .copied()
                    .filter(|element| element.local == "AssertionConsumerService")
                    .collect();
                lines.push(format!(
                    "authnRequestsSigned: {}",
                    attr("AuthnRequestsSigned") == "true"
                ));
                lines.push(format!(
                    "wantAssertionsSigned: {}",
                    attr("WantAssertionsSigned") == "true"
                ));
                lines.push(format!("keyDescriptorCount: {key_descriptor_count}"));
                lines.push(format!("acsCount: {}", services.len()));
                if let Some(acs) = services.first() {
                    let acs_attr = |name: &str| {
                        acs.attrs
                            .iter()
                            .find(|attr| attr.key == name)
                            .map(|attr| attr.value.clone())
                            .unwrap_or_default()
                    };
                    lines.push(format!("acs.binding: {}", acs_attr("Binding")));
                    lines.push(format!("acs.location: {}", acs_attr("Location")));
                    lines.push(format!(
                        "acs.index: {}",
                        acs_attr("index").parse::<i64>().unwrap_or(0)
                    ));
                }
            }
            lines
        }
        "acs" | "acs_multi" => {
            let provider_values = if kind == "acs" {
                vec![case.get("provider").cloned().unwrap()]
            } else {
                case.get("providers")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            };
            let site = site_with(external_url, provider_values.clone());
            let set = SamlProviderSet::from_site(
                &site,
                std::sync::Arc::new(FakeHttp {
                    status: 500,
                    body: Vec::new(),
                    error: false,
                }),
            );
            let sign_in: Vec<SignInProviderInfo> = provider_values
                .iter()
                .map(|provider| SignInProviderInfo {
                    provider_type: "saml".to_owned(),
                    config_id: config_for(provider, external_url)
                        .provider_config_id(provider_values.len() >= 2),
                    hidden: false,
                    no_sign_in: false,
                })
                .collect();
            let now = match case.get("userCreatedAt").and_then(Value::as_str) {
                Some(value) => parse_time(value),
                None => now,
            };
            let deps = SamlDeps {
                providers: &set,
                sign_in_providers: &sign_in,
                auth_public: false,
                external_url,
                store,
                ids,
                cookie_key: b"m9jt-cookie-key",
                secure: false,
                configured_expiry: None,
                now,
                provisioner: deps_extra.provisioner,
                users: deps_extra.users,
                suffix: deps_extra.suffix,
                sourcegraph_dotcom: false,
                license_expired: false,
            };
            let request = SamlHttpRequest {
                method: "POST".to_owned(),
                path: "/.auth/saml/acs".to_owned(),
                url_string: format!("{external_url}/.auth/saml/acs"),
                form: vec![
                    ("SAMLResponse".to_owned(), s(case, "encoded")),
                    ("RelayState".to_owned(), s(case, "relayState")),
                ],
                ..SamlHttpRequest::default()
            };
            let response = saml_middleware(&deps, &request)
                .await
                .expect("ACS endpoint responds");
            let mut lines = describe(&response);
            let name = s(case, "name");
            if matches!(
                name.as_str(),
                "e01_acs_existing_user" | "e02_acs_created_user" | "e05_acs_signup_disabled"
            ) {
                let recorded = deps_extra
                    .provisioner
                    .recorded
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_default();
                lines.extend(provision_lines(&recorded));
            }
            if name == "e01_acs_existing_user" {
                let config = config_for(case.get("provider").unwrap(), external_url);
                lines.push(format!(
                    "config.singleID: {}",
                    config.provider_config_id(false)
                ));
            }
            lines
        }
        "middleware" => {
            let provider = case.get("provider").cloned().unwrap();
            let site = site_with(external_url, vec![provider]);
            let set = SamlProviderSet::from_site(
                &site,
                std::sync::Arc::new(FakeHttp {
                    status: 500,
                    body: Vec::new(),
                    error: false,
                }),
            );
            let sign_in = sg_service_frontend::saml_auth::sign_in_providers_from_site(&site, false);
            let request_value = case.get("request").cloned().unwrap_or(Value::Null);
            let path = s(&request_value, "path");
            let deps = SamlDeps {
                providers: &set,
                sign_in_providers: &sign_in,
                auth_public: false,
                external_url,
                store,
                ids,
                cookie_key: b"m9jt-cookie-key",
                secure: false,
                configured_expiry: None,
                now,
                provisioner: deps_extra.provisioner,
                users: deps_extra.users,
                suffix: deps_extra.suffix,
                sourcegraph_dotcom: false,
                license_expired: false,
            };
            let method = s(&request_value, "method");
            let is_api = request_value
                .get("api")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let request = SamlHttpRequest {
                method: method.clone(),
                path: path.clone(),
                url_string: format!("{external_url}{path}"),
                has_sign_out_cookie: request_value
                    .get("hasSignOutCookie")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                authenticated: request_value
                    .get("authed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                is_api_request: is_api,
                ..SamlHttpRequest::default()
            };
            match saml_middleware(&deps, &request).await {
                Some(response) => {
                    let mut lines = vec![
                        format!("status: {}", response.status),
                        format!("location: {}", esc(&normalize_location(&response.location))),
                        format!(
                            "body: {}",
                            esc(&normalize_body(
                                response.status,
                                &String::from_utf8_lossy(&response.body)
                            ))
                        ),
                    ];
                    if !response.set_cookies.is_empty() {
                        lines.push(format!("cookies: {}", response.set_cookies.len()));
                        for (index, cookie) in response.set_cookies.iter().enumerate() {
                            lines.push(format!("cookie.{index}: {}", cookie_line(cookie)));
                        }
                    }
                    lines
                }
                None => vec![
                    "status: 200".to_owned(),
                    "location: ".to_owned(),
                    format!("body: {}", if is_api { "api-ok" } else { "app-ok" }),
                ],
            }
        }
        "logout" => {
            let provider = case.get("provider").cloned().unwrap();
            let site = site_with(external_url, vec![provider]);
            let set = SamlProviderSet::from_site(
                &site,
                std::sync::Arc::new(FakeHttp {
                    status: 500,
                    body: Vec::new(),
                    error: false,
                }),
            );
            let sign_in = vec![SignInProviderInfo {
                provider_type: "saml".to_owned(),
                config_id: String::new(),
                hidden: false,
                no_sign_in: false,
            }];
            let deps = SamlDeps {
                providers: &set,
                sign_in_providers: &sign_in,
                auth_public: false,
                external_url,
                store,
                ids,
                cookie_key: b"m9jt-cookie-key",
                secure: false,
                configured_expiry: None,
                now,
                provisioner: deps_extra.provisioner,
                users: deps_extra.users,
                suffix: deps_extra.suffix,
                sourcegraph_dotcom: false,
                license_expired: false,
            };
            let request = SamlHttpRequest {
                method: "POST".to_owned(),
                path: "/.auth/saml/logout".to_owned(),
                url_string: format!("{external_url}/.auth/saml/logout"),
                form: vec![("SAMLResponse".to_owned(), s(case, "encoded"))],
                ..SamlHttpRequest::default()
            };
            let response = saml_middleware(&deps, &request)
                .await
                .expect("logout endpoint responds");
            describe(&response)
        }
        other => panic!("unknown case kind {other}"),
    }
}

#[tokio::test]
async fn saml_auth_diff() {
    if std::env::var("SG_SAML_AUTH_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("SAML_AUTH_CORPUS").expect("SAML_AUTH_CORPUS");
    let out_dir = std::env::var("SAML_AUTH_OUT").expect("SAML_AUTH_OUT");
    let corpus: Value = serde_json::from_slice(&fs::read(&corpus_path).expect("read corpus"))
        .expect("parse corpus");
    let external_url = s(&corpus, "externalURL");
    let now = parse_time(&s(&corpus, "now"));
    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .expect("cases array");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let store = MemStore(Mutex::new(BTreeMap::new()));
    let ids = FixedIds;
    let suffix = NoSuffix;
    for case in cases {
        let name = s(case, "name");
        let mode = s(case, "provision");
        let safe_message = s(case, "safeMessage");
        let user_id = case.get("userID").and_then(Value::as_i64).unwrap_or(123) as i32;
        let provisioner = FakeProvisioner {
            mode: if mode.is_empty() {
                "existing".to_owned()
            } else {
                mode
            },
            user_id,
            safe_message,
            recorded: Mutex::new(None),
        };
        let created_at = case
            .get("userCreatedAt")
            .and_then(Value::as_str)
            .map(parse_time)
            .unwrap_or(now);
        let users = FixedUserStore {
            user_id,
            created_at,
        };
        let deps = Deps {
            provisioner: &provisioner,
            users: &users,
            suffix: &suffix,
        };
        let lines = run_case(case, &external_url, now, &store, &ids, &deps).await;
        let path = Path::new(&out_dir).join(format!("{name}.txt"));
        fs::write(path, format!("{}\n", lines.join("\n"))).expect("write expected output");
        println!("RUST {name}");
    }
}
