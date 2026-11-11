#![forbid(unsafe_code)]

//! `Site.authProviders` (`cmd/frontend/graphqlbackend/auth_providers.go:10-35`,
//! `auth_provider.go:6-35`) and the `auth.providers` site configuration driven
//! provider directory.
//!
//! The baseline registers one `providers.Provider` per configured entry from
//! each provider package's `conf.Watch` callback
//! (`cmd/frontend/internal/auth/*/config.go`), then `SortedProviders()` sorts
//! them for display. This module rebuilds that directory from the site
//! configuration snapshot without touching any provider's callback/token code:
//!
//! * OAuth (GitHub/GitLab/Azure DevOps/Bitbucket Cloud/Bitbucket Server),
//!   OpenID Connect, SAML and Gerrit derive `ConfigID`/`CachedInfo` exactly
//!   like the baseline;
//! * builtin and http-header are the two built-in providers;
//! * duplicate handling matches `providers.Update` (same package, same
//!   canonical `Config()` JSON) and the OAuth/Gerrit `UniqueID` filters;
//! * sorting matches `SortedProviders` for the deterministic cases (the
//!   baseline is unspecified when two providers share a non-zero `order`).
//!
//! Deliberate seams (overview §4):
//! * SAML ServiceID/ClientID stay empty because the baseline only fills them
//!   after fetching IdP metadata (`saml/provider.go:107-125`); this port does
//!   no network I/O.
//! * `licensing.Check(FeatureSSO)` is not modelled: the default free license
//!   enables SSO, so configured providers are listed (the baseline drops the
//!   whole package only for an explicitly invalid license).
//! * Invalid/relative provider URLs are dropped instead of panicking.

use base64::Engine;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use sg_config::SiteConfiguration;
use sg_graphql::exec::{Actor, Arguments, FieldValue, ObjectValue};
use sg_graphql::json::Json;

use crate::graphql_resolvers::PageInfoObject;

/// One row of `AuthProvider` (`schema.graphql:6588-6613`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthProviderInfo {
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub display_name: String,
    pub is_builtin: bool,
    pub authentication_url: Option<String>,
}

/// The prefetched `Site.authProviders` connection. The baseline always returns
/// `gqlutil.HasNextPage(false)` (`auth_providers.go:33`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthProvidersRoot {
    pub providers: Vec<AuthProviderInfo>,
}

impl AuthProvidersRoot {
    /// `siteResolver.AuthProviders` → `providers.SortedProviders()` →
    /// `CachedInfo()` (`auth_providers.go:10-31`).
    pub fn from_site(site: &SiteConfiguration) -> Self {
        let raw = site.raw();
        let external_url = raw
            .get("externalURL")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let entries = raw
            .get("auth.providers")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();

        let mut candidates: Vec<Candidate> = Vec::new();
        // `providers.Update` replaces a package's whole set, and
        // builtin/http-header resolve to the first config in the array
        // (`userpasswd/config.go:23-37`, `httpheader/config.go:17-27`).
        let mut builtin_seen = false;
        let mut http_header_seen = false;
        // Per-package dedup keys: OAuth/gerrit by `CachedInfo().UniqueID()`
        // (`providers.go:99-101`), OIDC/SAML by the canonical `Config()` JSON
        // (`providers.go:118-140`).
        let mut oauth_seen: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        let mut gerrit_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut canonical_seen: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // The SAML `multiple` flag depends on the number of SAML entries, so
        // resolve it before iterating (`saml/config.go:101-114`).
        let saml_multiple = entries
            .iter()
            .filter(|entry| entry.get("type").and_then(Value::as_str) == Some("saml"))
            .count()
            >= 2;

        for entry in entries {
            let Some(obj) = entry.as_object() else {
                continue;
            };
            match obj.get("type").and_then(Value::as_str).unwrap_or_default() {
                "builtin" => {
                    if builtin_seen {
                        continue;
                    }
                    builtin_seen = true;
                    candidates.push(Candidate {
                        config_type: "builtin".to_owned(),
                        config_id: String::new(),
                        order: 0,
                        info: AuthProviderInfo {
                            service_type: "builtin".to_owned(),
                            service_id: String::new(),
                            client_id: String::new(),
                            display_name: "Builtin username-password authentication".to_owned(),
                            is_builtin: true,
                            authentication_url: None,
                        },
                    });
                }
                "http-header" => {
                    if http_header_seen {
                        continue;
                    }
                    http_header_seen = true;
                    let username_header = string_field(obj, "usernameHeader");
                    candidates.push(Candidate {
                        config_type: "http-header".to_owned(),
                        config_id: String::new(),
                        order: 0,
                        info: AuthProviderInfo {
                            service_type: "http-header".to_owned(),
                            service_id: String::new(),
                            client_id: String::new(),
                            display_name: format!(
                                "HTTP authentication proxy ({} header)",
                                go_quote(&canonical_mime_header_key(&username_header))
                            ),
                            is_builtin: false,
                            authentication_url: None,
                        },
                    });
                }
                "github" | "gitlab" | "azureDevOps" | "bitbucketcloud" | "bitbucketserver" => {
                    // `gitlaboauth.parseConfig` drops every GitLab entry when
                    // `externalURL` is empty (`gitlaboauth/config.go:62-65`):
                    // it cannot build the OAuth callback URL.
                    if obj.get("type").and_then(Value::as_str) == Some("gitlab")
                        && external_url.is_empty()
                    {
                        continue;
                    }
                    if let Some((service_type, service_id, client_id, prefix)) =
                        oauth_identity(obj, entry)
                    {
                        if !oauth_seen.insert((service_id.clone(), client_id.clone())) {
                            continue;
                        }
                        candidates.push(Candidate {
                            config_type: service_type.clone(),
                            config_id: format!("{service_id}::{client_id}"),
                            order: int_field(obj, "order"),
                            info: AuthProviderInfo {
                                display_name: {
                                    let configured = string_field(obj, "displayName");
                                    if configured.is_empty() {
                                        service_id.clone()
                                    } else {
                                        configured
                                    }
                                },
                                service_type,
                                service_id: service_id.clone(),
                                client_id: client_id.clone(),
                                is_builtin: false,
                                authentication_url: Some(login_url(
                                    prefix,
                                    &format!("{service_id}::{client_id}"),
                                )),
                            },
                        });
                    }
                }
                "gerrit" => {
                    let url = string_field(obj, "url");
                    if !gerrit_seen.insert(url.clone()) {
                        continue;
                    }
                    // `Config()` reconstructs only Type/Url, so the common
                    // `Order`/`DisplayName` fields are always zero
                    // (`gerrit/config.go:72-88`).
                    candidates.push(Candidate {
                        config_type: "gerrit".to_owned(),
                        config_id: url.clone(),
                        order: 0,
                        info: AuthProviderInfo {
                            service_type: "gerrit".to_owned(),
                            service_id: url.clone(),
                            client_id: String::new(),
                            display_name: "Gerrit".to_owned(),
                            is_builtin: false,
                            authentication_url: if url.is_empty() { None } else { Some(url) },
                        },
                    });
                }
                "openidconnect" => {
                    let canonical = go_json_oidc(obj);
                    if !canonical_seen.insert(canonical.clone()) {
                        continue;
                    }
                    let config_id = config_id_or_hash(obj, &canonical);
                    let issuer = string_field(obj, "issuer");
                    let client_id = string_field(obj, "clientID");
                    let display_name = {
                        let configured = string_field(obj, "displayName");
                        if configured.is_empty() {
                            "OpenID Connect".to_owned()
                        } else {
                            configured
                        }
                    };
                    let service_type = string_field(obj, "type");
                    candidates.push(Candidate {
                        config_type: service_type.clone(),
                        config_id: config_id.clone(),
                        order: int_field(obj, "order"),
                        info: AuthProviderInfo {
                            service_type,
                            service_id: issuer,
                            client_id,
                            display_name,
                            is_builtin: false,
                            authentication_url: Some(login_url("/.auth/openidconnect", &config_id)),
                        },
                    });
                }
                "saml" => {
                    let issuer_default = saml_service_provider_issuer(obj, external_url);
                    let canonical = go_json_saml(obj, &issuer_default);
                    if !canonical_seen.insert(canonical.clone()) {
                        continue;
                    }
                    let config_id = match obj.get("configID").and_then(Value::as_str) {
                        Some(id) if !id.is_empty() => id.to_owned(),
                        _ if saml_multiple => hash_config_id(&canonical),
                        _ => String::new(),
                    };
                    let display_name = {
                        let configured = string_field(obj, "displayName");
                        if configured.is_empty() {
                            "SAML".to_owned()
                        } else {
                            configured
                        }
                    };
                    let authentication_url = if saml_multiple {
                        login_url("/.auth/saml", &config_id)
                    } else {
                        "/.auth/saml/login".to_owned()
                    };
                    candidates.push(Candidate {
                        config_type: "saml".to_owned(),
                        config_id,
                        order: int_field(obj, "order"),
                        info: AuthProviderInfo {
                            service_type: "saml".to_owned(),
                            service_id: String::new(),
                            client_id: String::new(),
                            display_name,
                            is_builtin: false,
                            authentication_url: Some(authentication_url),
                        },
                    });
                }
                // Unknown discriminant: the baseline rejects the whole site
                // configuration before it ever starts.
                _ => {}
            }
        }

        candidates.sort_by(|a, b| {
            if candidate_less(a, b) {
                std::cmp::Ordering::Less
            } else if candidate_less(b, a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        Self {
            providers: candidates.into_iter().map(|c| c.info).collect(),
        }
    }
}

struct Candidate {
    config_type: String,
    config_id: String,
    order: i32,
    info: AuthProviderInfo,
}

/// `SortedProviders`' comparator (`providers.go:216-251`).
fn candidate_less(a: &Candidate, b: &Candidate) -> bool {
    let (order_i, order_j) = (a.order, b.order);
    if order_i != 0 && order_j != 0 {
        return order_i < order_j;
    }
    if order_i != 0 || order_j != 0 {
        return order_j == 0;
    }
    if a.config_type == "builtin" && b.config_type != "builtin" {
        return true;
    }
    if b.config_type == "builtin" && a.config_type != "builtin" {
        return false;
    }
    if a.config_type != b.config_type {
        return a.config_type < b.config_type;
    }
    a.config_id < b.config_id
}

/// `oauth.Provider.CachedInfo`'s identity fields and login prefix.
fn oauth_identity(
    obj: &Map<String, Value>,
    entry: &Value,
) -> Option<(String, String, String, &'static str)> {
    let _ = entry;
    match obj.get("type").and_then(Value::as_str).unwrap_or_default() {
        "github" => {
            let url = match obj.get("url").and_then(Value::as_str) {
                Some(url) if !url.is_empty() => url.to_owned(),
                _ => "https://github.com/".to_owned(),
            };
            Some((
                "github".to_owned(),
                normalize_base_url(&url)?,
                string_field(obj, "clientID"),
                "/.auth/github",
            ))
        }
        "gitlab" => {
            let url = match obj.get("url").and_then(Value::as_str) {
                Some(url) if !url.is_empty() => url.to_owned(),
                _ => "https://gitlab.com/".to_owned(),
            };
            Some((
                "gitlab".to_owned(),
                normalize_base_url(&url)?,
                string_field(obj, "clientID"),
                "/.auth/gitlab",
            ))
        }
        // `azuredevops.AzureDevOpsAPIURL` (`internal/extsvc/azuredevops/client.go:26`).
        "azureDevOps" => Some((
            "azuredevops".to_owned(),
            "https://dev.azure.com/".to_owned(),
            string_field(obj, "clientID"),
            "/.auth/azuredevops",
        )),
        "bitbucketcloud" => {
            let url = match obj.get("url").and_then(Value::as_str) {
                Some(url) if !url.is_empty() => url.to_owned(),
                _ => "https://bitbucket.org".to_owned(),
            };
            Some((
                "bitbucketCloud".to_owned(),
                normalize_base_url(&url)?,
                string_field(obj, "clientKey"),
                "/.auth/bitbucketcloud",
            ))
        }
        "bitbucketserver" => Some((
            "bitbucketServer".to_owned(),
            normalize_base_url(&string_field(obj, "url"))?,
            string_field(obj, "clientID"),
            "/.auth/bitbucketserver",
        )),
        _ => None,
    }
}

/// `extsvc.NormalizeBaseURL` (`internal/extsvc/codehost.go:86-92`): lowercase
/// the host and ensure the path ends with `/`. `url.URL.String()` output.
fn normalize_base_url(raw: &str) -> Option<String> {
    let mut url = url::Url::parse(raw).ok()?;
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Some(url.to_string())
}

/// `url.Values.Encode` + `url.URL{Path, RawQuery}.String()` for the login URLs.
fn login_url(prefix: &str, config_id: &str) -> String {
    format!("{prefix}/login?pc={}", query_escape(config_id))
}

/// Go `url.QueryEscape` (`net/url`): alphanumerics and `-_.~` are literal,
/// space becomes `+`, everything else is `%XX` with uppercase hex.
fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn string_field(obj: &Map<String, Value>, key: &str) -> String {
    obj.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// Go `int` (the JSON number must be integral; a fractional value would make
/// the baseline reject the configuration).
fn int_field(obj: &Map<String, Value>, key: &str) -> i32 {
    obj.get(key)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(0)
}

fn config_id_or_hash(obj: &Map<String, Value>, canonical: &str) -> String {
    match obj.get("configID").and_then(Value::as_str) {
        Some(id) if !id.is_empty() => id.to_owned(),
        _ => hash_config_id(canonical),
    }
}

/// `base64.RawURLEncoding.EncodeToString(sha256(data)[:16])`
/// (`openidconnect/config.go:134-145`, `saml/config.go:176-189`).
fn hash_config_id(canonical: &str) -> String {
    let digest = Sha256::digest(canonical.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest[..16])
}

/// `saml.withConfigDefaults` (`saml/config.go:144-158`).
fn saml_service_provider_issuer(obj: &Map<String, Value>, external_url: &str) -> String {
    let configured = string_field(obj, "serviceProviderIssuer");
    if !configured.is_empty() || external_url.is_empty() {
        return configured;
    }
    // Go `strings.TrimSuffix(externalURL, "/")` removes at most one slash.
    format!(
        "{}/.auth/saml/metadata",
        external_url.strip_suffix('/').unwrap_or(external_url)
    )
}

/// `textproto.CanonicalMIMEHeaderKey` (`net/textproto`).
fn canonical_mime_header_key(value: &str) -> String {
    let valid = |b: u8| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    };
    let mut bytes = value.as_bytes().to_vec();
    let mut upper = true;
    for byte in &mut bytes {
        if !valid(*byte) {
            return value.to_owned();
        }
        if upper && byte.is_ascii_lowercase() {
            *byte = byte.to_ascii_uppercase();
        } else if !upper && byte.is_ascii_uppercase() {
            *byte = byte.to_ascii_lowercase();
        }
        upper = *byte == b'-';
    }
    String::from_utf8(bytes).unwrap_or_else(|_| value.to_owned())
}

/// `fmt.Sprintf("%q", s)`: Go string-literal quoting. Normal header names need
/// only the surrounding quotes; unusual bytes fall back to Rust's escaping
/// (documented deviation, never hit by valid HTTP header names).
fn go_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_graphic() && ch != '"' && ch != '\\')
    {
        return format!("\"{value}\"");
    }
    let escaped: String = value.chars().flat_map(|ch| ch.escape_default()).collect();
    format!("\"{escaped}\"")
}

// ---------------------------------------------------------------------------
// Go-compatible `json.Marshal` for the config-ID hashes.
// ---------------------------------------------------------------------------

/// One field of the canonical struct JSON, in Go declaration order.
type GoFields = Vec<(&'static str, String)>;

fn go_string(value: &str) -> String {
    serde_json::to_string(value).expect("string serializes")
}

fn push_string(fields: &mut GoFields, key: &'static str, value: &str, omit_empty: bool) {
    if omit_empty && value.is_empty() {
        return;
    }
    fields.push((key, go_string(value)));
}

fn push_bool(fields: &mut GoFields, key: &'static str, value: bool, omit_false: bool) {
    if omit_false && !value {
        return;
    }
    fields.push((key, if value { "true" } else { "false" }.to_owned()));
}

fn push_int(fields: &mut GoFields, key: &'static str, value: i32, omit_zero: bool) {
    if omit_zero && value == 0 {
        return;
    }
    fields.push((key, value.to_string()));
}

/// Pointer fields (`*bool`/`*string`): omitted only when the JSON key is
/// absent or `null`, otherwise emitted even for the zero value.
fn push_pointer_bool(fields: &mut GoFields, key: &'static str, obj: &Map<String, Value>) {
    if let Some(value) = obj.get(key).and_then(Value::as_bool) {
        fields.push((key, if value { "true" } else { "false" }.to_owned()));
    }
}

fn push_pointer_string(fields: &mut GoFields, key: &'static str, obj: &Map<String, Value>) {
    if let Some(value) = obj.get(key).and_then(Value::as_str) {
        fields.push((key, go_string(value)));
    }
}

/// `[]string` with `omitempty`: absent/null/empty → omitted.
fn push_string_slice(fields: &mut GoFields, key: &'static str, obj: &Map<String, Value>) {
    let Some(items) = obj.get(key).and_then(Value::as_array) else {
        return;
    };
    if items.is_empty() {
        return;
    }
    let serialized = serde_json::to_string(items).expect("array serializes");
    fields.push((key, serialized));
}

fn render_go_json(fields: GoFields) -> String {
    let mut out = String::from("{");
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&go_string(key));
        out.push(':');
        out.push_str(value);
    }
    out.push('}');
    escape_go_html(&out)
}

/// Go's `encoding/json` escapes `<`, `>`, `&`, U+2028 and U+2029 even inside
/// string values; those bytes cannot occur outside JSON strings, so a global
/// replacement is equivalent.
fn escape_go_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ => out.push(ch),
        }
    }
    out
}

/// `schema.OpenIDConnectAuthProvider` (field declaration order).
fn go_json_oidc(obj: &Map<String, Value>) -> String {
    let mut fields = GoFields::new();
    push_pointer_bool(&mut fields, "allowSignup", obj);
    push_string(
        &mut fields,
        "clientID",
        &string_field(obj, "clientID"),
        false,
    );
    push_string(
        &mut fields,
        "clientSecret",
        &string_field(obj, "clientSecret"),
        false,
    );
    push_string(
        &mut fields,
        "configID",
        &string_field(obj, "configID"),
        true,
    );
    push_string(
        &mut fields,
        "displayName",
        &string_field(obj, "displayName"),
        true,
    );
    push_pointer_string(&mut fields, "displayPrefix", obj);
    push_bool(&mut fields, "hidden", bool_field(obj, "hidden"), true);
    push_string(&mut fields, "issuer", &string_field(obj, "issuer"), false);
    push_bool(&mut fields, "noSignIn", bool_field(obj, "noSignIn"), true);
    push_int(&mut fields, "order", int_field(obj, "order"), true);
    push_string(
        &mut fields,
        "requireEmailDomain",
        &string_field(obj, "requireEmailDomain"),
        true,
    );
    push_bool(
        &mut fields,
        "singleIdentityPerUser",
        bool_field(obj, "singleIdentityPerUser"),
        true,
    );
    push_string(&mut fields, "type", &string_field(obj, "type"), false);
    render_go_json(fields)
}

/// `schema.SAMLAuthProvider` (field declaration order), after
/// `withConfigDefaults` has filled `serviceProviderIssuer`.
fn go_json_saml(obj: &Map<String, Value>, default_issuer: &str) -> String {
    let mut fields = GoFields::new();
    push_string_slice(&mut fields, "allowGroups", obj);
    push_pointer_bool(&mut fields, "allowSignup", obj);
    push_string(
        &mut fields,
        "configID",
        &string_field(obj, "configID"),
        true,
    );
    push_string(
        &mut fields,
        "displayName",
        &string_field(obj, "displayName"),
        true,
    );
    push_pointer_string(&mut fields, "displayPrefix", obj);
    push_string(
        &mut fields,
        "groupsAttributeName",
        &string_field(obj, "groupsAttributeName"),
        true,
    );
    push_bool(&mut fields, "hidden", bool_field(obj, "hidden"), true);
    push_string(
        &mut fields,
        "identityProviderMetadata",
        &string_field(obj, "identityProviderMetadata"),
        true,
    );
    push_string(
        &mut fields,
        "identityProviderMetadataURL",
        &string_field(obj, "identityProviderMetadataURL"),
        true,
    );
    push_bool(
        &mut fields,
        "insecureSkipAssertionSignatureValidation",
        bool_field(obj, "insecureSkipAssertionSignatureValidation"),
        true,
    );
    push_string(
        &mut fields,
        "nameIDFormat",
        &string_field(obj, "nameIDFormat"),
        true,
    );
    push_bool(&mut fields, "noSignIn", bool_field(obj, "noSignIn"), true);
    push_int(&mut fields, "order", int_field(obj, "order"), true);
    push_string(
        &mut fields,
        "serviceProviderCertificate",
        &string_field(obj, "serviceProviderCertificate"),
        true,
    );
    // `withConfigDefaults` only fills an empty issuer; emit the defaulted value.
    let issuer = string_field(obj, "serviceProviderIssuer");
    let issuer = if issuer.is_empty() {
        default_issuer.to_owned()
    } else {
        issuer
    };
    push_string(&mut fields, "serviceProviderIssuer", &issuer, true);
    push_string(
        &mut fields,
        "serviceProviderPrivateKey",
        &string_field(obj, "serviceProviderPrivateKey"),
        true,
    );
    push_pointer_bool(&mut fields, "signRequests", obj);
    push_string(&mut fields, "type", &string_field(obj, "type"), false);
    push_string_slice(&mut fields, "usernameAttributeNames", obj);
    render_go_json(fields)
}

fn bool_field(obj: &Map<String, Value>, key: &str) -> bool {
    obj.get(key).and_then(Value::as_bool).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// GraphQL objects
// ---------------------------------------------------------------------------

/// `authProviderResolver` (`auth_provider.go:6-35`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProviderObject {
    info: AuthProviderInfo,
}

impl AuthProviderObject {
    pub fn new(info: AuthProviderInfo) -> Self {
        Self { info }
    }
}

impl ObjectValue for AuthProviderObject {
    fn type_name(&self) -> &'static str {
        "AuthProvider"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, sg_graphql::exec::ExecError> {
        match field {
            "serviceType" => Ok(Some(FieldValue::Json(Json::Str(
                self.info.service_type.clone(),
            )))),
            "serviceID" => Ok(Some(FieldValue::Json(Json::Str(
                self.info.service_id.clone(),
            )))),
            "clientID" => Ok(Some(FieldValue::Json(Json::Str(
                self.info.client_id.clone(),
            )))),
            "displayName" => Ok(Some(FieldValue::Json(Json::Str(
                self.info.display_name.clone(),
            )))),
            "isBuiltin" => Ok(Some(FieldValue::Json(Json::Bool(self.info.is_builtin)))),
            "authenticationURL" => Ok(Some(match &self.info.authentication_url {
                Some(url) => FieldValue::Json(Json::Str(url.clone())),
                None => FieldValue::Json(Json::Null),
            })),
            _ => Ok(None),
        }
    }
}

/// `authProviderConnectionResolver` (`auth_providers.go:17-35`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProviderConnectionObject {
    providers: Vec<AuthProviderInfo>,
}

impl AuthProviderConnectionObject {
    pub fn new(providers: Vec<AuthProviderInfo>) -> Self {
        Self { providers }
    }
}

impl ObjectValue for AuthProviderConnectionObject {
    fn type_name(&self) -> &'static str {
        "AuthProviderConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, sg_graphql::exec::ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .providers
                    .iter()
                    .cloned()
                    .map(|info| {
                        FieldValue::Object(std::sync::Arc::new(AuthProviderObject::new(info)))
                    })
                    .collect(),
                element_type: Some("AuthProvider"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                i64::try_from(self.providers.len()).unwrap_or(i64::MAX),
            )))),
            // `gqlutil.HasNextPage(false)` (`auth_providers.go:33-34`).
            "pageInfo" => Ok(Some(FieldValue::Object(std::sync::Arc::new(
                PageInfoObject::new(false, None),
            )))),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    fn names(root: &AuthProvidersRoot) -> Vec<(&str, &str, &str)> {
        root.providers
            .iter()
            .map(|p| {
                (
                    p.service_type.as_str(),
                    p.display_name.as_str(),
                    p.authentication_url.as_deref().unwrap_or(""),
                )
            })
            .collect()
    }

    #[test]
    fn empty_configuration_has_no_providers() {
        assert_eq!(AuthProvidersRoot::from_site(&site("{}")).providers, vec![]);
        assert_eq!(
            AuthProvidersRoot::from_site(&site(r#"{"auth.providers":[]}"#)).providers,
            vec![]
        );
    }

    #[test]
    fn builtin_and_http_header_match_the_baseline() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[{"type":"http-header","usernameHeader":"x-forwarded-user"},{"type":"builtin"}]}"#,
        ));
        // builtin sorts first even though http-header came first.
        assert_eq!(
            names(&root),
            vec![
                ("builtin", "Builtin username-password authentication", ""),
                (
                    "http-header",
                    "HTTP authentication proxy (\"X-Forwarded-User\" header)",
                    ""
                ),
            ]
        );
        assert!(root.providers[0].is_builtin);
    }

    #[test]
    fn github_defaults_and_urls() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[{"type":"github","clientID":"cid","clientSecret":"sec"}]}"#,
        ));
        let p = &root.providers[0];
        assert_eq!(p.service_type, "github");
        assert_eq!(p.service_id, "https://github.com/");
        assert_eq!(p.display_name, "https://github.com/");
        assert_eq!(
            p.authentication_url.as_deref(),
            Some("/.auth/github/login?pc=https%3A%2F%2Fgithub.com%2F%3A%3Acid")
        );

        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[{"type":"github","url":"https://GitHub.Example.com","clientID":"cid","clientSecret":"sec","displayName":"GH"}]}"#,
        ));
        assert_eq!(root.providers[0].service_id, "https://github.example.com/");
        assert_eq!(root.providers[0].display_name, "GH");
    }

    #[test]
    fn gitlab_requires_external_url() {
        let without =
            r#"{"auth.providers":[{"type":"gitlab","clientID":"cid","clientSecret":"sec"}]}"#;
        assert_eq!(
            AuthProvidersRoot::from_site(&site(without)).providers,
            vec![]
        );
        let with = r#"{"externalURL":"https://sg.example.com","auth.providers":[{"type":"gitlab","clientID":"cid","clientSecret":"sec"}]}"#;
        assert_eq!(
            AuthProvidersRoot::from_site(&site(with)).providers[0].service_id,
            "https://gitlab.com/"
        );
    }

    #[test]
    fn azure_devops_service_id_is_the_api_url() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[{"type":"azureDevOps","clientID":"cid","clientSecret":"sec"}]}"#,
        ));
        assert_eq!(root.providers[0].service_id, "https://dev.azure.com/");
    }

    #[test]
    fn oidc_config_id_hash_matches_go() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[{"type":"openidconnect","issuer":"https://issuer.example.com","clientID":"cid","clientSecret":"csec"}]}"#,
        ));
        assert_eq!(
            root.providers[0].authentication_url.as_deref(),
            Some("/.auth/openidconnect/login?pc=-IEG8L1ci6LUfFQCH0cZ_w")
        );
    }

    #[test]
    fn saml_hashes_the_defaulted_config_for_multiple_providers() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"externalURL":"https://example.com","auth.providers":[{"type":"saml","displayName":"One"},{"type":"saml","displayName":"Two"}]}"#,
        ));
        assert_eq!(
            names(&root),
            vec![
                ("saml", "Two", "/.auth/saml/login?pc=aISgV4xmap7FpVomfANRAA"),
                ("saml", "One", "/.auth/saml/login?pc=hwwyXiVdVdF23bUu4VhynQ"),
            ]
        );
        assert_eq!(root.providers[0].service_id, "");
        assert_eq!(root.providers[0].client_id, "");
    }

    #[test]
    fn sorting_uses_order_then_builtin_then_type_and_id() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"externalURL":"https://sg.example.com","auth.providers":[
                {"type":"github","clientID":"b","clientSecret":"s"},
                {"type":"builtin"},
                {"type":"gitlab","clientID":"a","clientSecret":"s","order":2},
                {"type":"openidconnect","issuer":"https://x","clientID":"c","clientSecret":"s","configID":"o","order":1}
            ]}"#,
        ));
        let types: Vec<&str> = root
            .providers
            .iter()
            .map(|p| p.service_type.as_str())
            .collect();
        assert_eq!(types, vec!["openidconnect", "gitlab", "builtin", "github"]);
    }

    #[test]
    fn gerrit_ignores_common_config_fields() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[{"type":"gerrit","url":"https://gerrit.example.com","displayName":"G","order":5}]}"#,
        ));
        assert_eq!(root.providers[0].display_name, "Gerrit");
        assert_eq!(
            root.providers[0].authentication_url.as_deref(),
            Some("https://gerrit.example.com")
        );
    }

    #[test]
    fn oauth_duplicates_keep_the_first_instance() {
        let root = AuthProvidersRoot::from_site(&site(
            r#"{"auth.providers":[
                {"type":"github","clientID":"cid","clientSecret":"s1","displayName":"first"},
                {"type":"github","clientID":"cid","clientSecret":"s2","displayName":"second"}
            ]}"#,
        ));
        assert_eq!(root.providers.len(), 1);
        assert_eq!(root.providers[0].display_name, "first");
    }

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(query_escape("a b:c/d+e~f"), "a+b%3Ac%2Fd%2Be~f");
    }

    #[test]
    fn canonical_header_key_matches_go() {
        assert_eq!(
            canonical_mime_header_key("x-forwarded-user"),
            "X-Forwarded-User"
        );
        assert_eq!(
            canonical_mime_header_key("X-FORWARDED-USER"),
            "X-Forwarded-User"
        );
        assert_eq!(canonical_mime_header_key("bad header"), "bad header");
    }
}
