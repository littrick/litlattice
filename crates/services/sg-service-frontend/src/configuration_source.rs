#![forbid(unsafe_code)]

//! Port of the DB-backed `configurationSource` in
//! `cmd/frontend/internal/cli/config.go:454-484`.
//!
//! [`DbConfigurationSource`] implements
//! [`sg_config::server::ConfigurationSource`]: `read` calls
//! `critical_and_site_config`'s `SiteGetLatest` (seeding the deployment
//! default when the table is empty, M9.8eg) and attaches the environment
//! driven `serviceConnections` (M9.8eh), producing the `RawUnified` that the
//! configuration service hands to the rest of the process. `write` follows
//! `WriteWithOverride` (M9.8ej): read latest, reject a stale `last_id`, then
//! `SiteCreateIfUpToDate`.
//!
//! Errata (see the M9.8ei, M9.8ej and M9.19c overviews):
//! * the read path has no actor context, so the seeding insert uses author 0
//!   (stored as NULL) like the baseline's unauthenticated reads;
//! * `ValidateSite`/`Validate`'s JSON Schema and custom checks are not
//!   ported, so writes are only syntax-checked;
//! * `conf.RedactAndHashSecrets` (M9.19c) is ported, including Go's
//!   schema-driven re-serialization of `auth.providers` /
//!   `observability.alerts` and the `siteConfigSecrets` edit paths;
//!   `UnredactSecrets` is not ported. A redaction failure still stores an
//!   empty `redacted_contents`, like the baseline's fallback;
//! * frontend startup wiring (connection pool creation,
//!   `InitConfigurationServerFrontendOnly`, the update loop and fsnotify) is
//!   still not connected.

use sg_config::server::ConfigurationSource;
use sg_config::RawUnified;
use sg_db::conf as conf_db;
use sg_jsonx::{
    apply_edits, compute_property_edit, format, go_json_string, parse, property_path, Edit,
    EditValue, FormatOptions, JsonValue, ParseOptions, Path,
};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::collections::BTreeMap;

use crate::{deploy_defaults, service_connections};

/// `config.go`'s `configurationSource` (DB-backed).
///
/// Cloning shares the pool; the watcher (M9.8ge) gets its own handle while the
/// configuration server keeps the original.
#[derive(Clone)]
pub struct DbConfigurationSource {
    pool: PgPool,
    default_site: &'static str,
    default_redacted: &'static str,
}

impl DbConfigurationSource {
    /// Resolve the deployment defaults from `DEPLOY_TYPE` at construction,
    /// mirroring `defaultConfigForDeployment` at package init (an invalid
    /// deployment type is the same fatal-text error).
    pub fn new(pool: PgPool) -> Result<Self, String> {
        let getenv: &dyn Fn(&str) -> Option<String> = &|key| std::env::var(key).ok();
        let deploy_type = service_connections::deploy_type(getenv);
        Self::with_deploy_type(pool, &deploy_type)
    }

    pub fn with_deploy_type(pool: PgPool, deploy_type: &str) -> Result<Self, String> {
        let (default_site, default_redacted) =
            deploy_defaults::default_site_config_pair(deploy_type)?;
        Ok(Self {
            pool,
            default_site,
            default_redacted,
        })
    }

    /// `configurationSource.Read` with the acting user's ID for the seeding
    /// insert (0 without an actor, which stores NULL).
    pub async fn read_with_author(&self, author_user_id: i32) -> Result<RawUnified, String> {
        let site = conf_db::site_get_latest(
            &self.pool,
            author_user_id,
            self.default_site,
            self.default_redacted,
        )
        .await
        .map_err(|err| format!("ConfStore.SiteGetLatest: {err}"))?;

        Ok(RawUnified {
            id: site.id,
            site: site.contents,
            service_connections: service_connections::service_connections()?,
        })
    }

    /// `configurationSource.WriteWithOverride`.
    ///
    /// The `isOverride` flag selects `conf.Validate` (true) or
    /// `conf.ValidateSite` (false) in the baseline; neither's schema checks
    /// are ported, so both paths only syntax-check the document (see the
    /// M9.8ej overview).
    pub async fn write_with_override(
        &self,
        data: RawUnified,
        last_id: i32,
        author_user_id: i32,
        _is_override: bool,
    ) -> Result<(), String> {
        // `WriteWithOverride` reads through `SiteGetLatest(ctx)`, whose seed
        // author comes from the actor in the context (0 here), not from the
        // write's `authorUserID`.
        let site =
            conf_db::site_get_latest(&self.pool, 0, self.default_site, self.default_redacted)
                .await
                .map_err(|err| format!("ConfStore.SiteGetLatest: {err}"))?;
        if site.id != last_id {
            return Err(
                "site config has been modified by another request, write not allowed".to_owned(),
            );
        }

        validate_site(&data.site)?;

        // `conf.RedactAndHashSecrets`: redaction failures are not fatal; the
        // insert falls back to an empty `redacted_contents`, exactly like
        // `database.confStore.createIfUpToDate`.
        let redacted = redact_and_hash_secrets(&data.site).unwrap_or_default();
        conf_db::site_create_if_up_to_date(
            &self.pool,
            Some(last_id),
            author_user_id,
            &data.site,
            &redacted,
            self.default_site,
            self.default_redacted,
        )
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "SiteConfig creation failed");
            format!("ConfStore.SiteCreateIfUpToDate: {err}")
        })?;
        Ok(())
    }
}

/// `redactHashString`'s prefix.
const REDACTED_HASH_PREFIX: &str = "REDACTED-DATA-CHUNK-";

/// `siteConfigSecrets` (`internal/conf/validate.go:205-231`). The `editPaths`
/// are `jsonx.PropertyPath` segments: each element is a *literal* property
/// name, so keys like `"auth.providers"` and `"email.smtp"` keep their dots
/// (Go's `readPath` escapes exactly those dots for gjson).
const SITE_CONFIG_SECRETS: &[&[&str]] = &[
    &["executors.accessToken"],
    &["email.smtp", "username"],
    &["email.smtp", "password"],
    &["organizationInvitations", "signingKey"],
    &["githubClientSecret"],
    &["dotcom", "githubApp.cloud", "clientSecret"],
    &["dotcom", "githubApp.cloud", "privateKey"],
    &["gitHubApp", "privateKey"],
    &["gitHubApp", "clientSecret"],
    &["auth.unlockAccountLinkSigningKey"],
    &["dotcom", "srcCliVersionCache", "github", "token"],
    &["dotcom", "srcCliVersionCache", "github", "webhookSecret"],
    &["dotcom", "sams.clientSecret"],
    &["embeddings", "accessToken"],
    &["completions", "accessToken"],
    &["app", "dotcomAuthToken"],
    &["attribution.gateway", "accessToken"],
    &["scim.authToken"],
];

/// `redactHashString`: `REDACTED-DATA-CHUNK-` + the first 10 hex digits of
/// SHA-256.
pub fn redact_hash_string(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{REDACTED_HASH_PREFIX}{}", &hex[..10])
}

/// `jsonc.DefaultFormatOptions` (`{InsertSpaces: true, TabSize: 2}`).
fn jsonc_format_options() -> FormatOptions {
    FormatOptions {
        tab_size: 2,
        insert_spaces: true,
        eol: String::new(),
    }
}

fn parse_jsonc(text: &str) -> Result<JsonValue, String> {
    let (value, errors) = parse(
        text,
        ParseOptions {
            comments: true,
            trailing_commas: true,
        },
    );
    if let Some(error) = errors.first() {
        return Err(format!("parse config: {}", error.code_name()));
    }
    value.ok_or_else(|| "parse config: unexpected end of JSON input".to_owned())
}

fn value_at<'a>(value: &'a JsonValue, path: &[&str]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path {
        match current {
            JsonValue::Object(object) => current = object.get(*segment)?,
            _ => return None,
        }
    }
    Some(current)
}

fn string_at(value: &JsonValue, path: &[&str]) -> Option<String> {
    match value_at(value, path)? {
        JsonValue::String(text) => Some(text.clone()),
        _ => None,
    }
}

/// How a string field is transformed before it is written back.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HashMode {
    /// Leave the value untouched.
    None,
    /// `getRedactedSecret(secret, true)` — always hash, even `""`.
    Always,
    /// Only hash a non-empty value (the observability notifiers).
    IfNonEmpty,
}

/// The Go field kinds that appear in the schema structs we re-serialize.
/// `*bool`/`*string` (pointer + `omitempty`) are distinct from the plain
/// values: a present pointer to `false`/`""` is still emitted.
enum FieldKind {
    StrRequired,
    StrOptional,
    StrPtr,
    Bool,
    BoolPtr,
    Int,
    StrList,
    StrListMap,
    Responders,
}

struct FieldSpec {
    name: &'static str,
    kind: FieldKind,
    hash: HashMode,
}

const fn fs(name: &'static str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name,
        kind,
        hash: HashMode::None,
    }
}

const fn fh(name: &'static str, kind: FieldKind, hash: HashMode) -> FieldSpec {
    FieldSpec { name, kind, hash }
}

const fn str_req(name: &'static str) -> FieldSpec {
    fs(name, FieldKind::StrRequired)
}

const fn str_opt(name: &'static str) -> FieldSpec {
    fs(name, FieldKind::StrOptional)
}

const fn str_ptr(name: &'static str) -> FieldSpec {
    fs(name, FieldKind::StrPtr)
}

/// `schema.AzureDevOpsAuthProvider` in struct-declaration order (the order
/// `json.Marshal` emits).
const AZURE_DEVOPS_FIELDS: &[FieldSpec] = &[
    fs("allowOrgs", FieldKind::StrList),
    fs("allowSignup", FieldKind::BoolPtr),
    str_opt("apiScope"),
    str_req("clientID"),
    fh("clientSecret", FieldKind::StrRequired, HashMode::Always),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_req("type"),
];

const BITBUCKETCLOUD_FIELDS: &[FieldSpec] = &[
    fs("allowSignup", FieldKind::Bool),
    str_opt("apiScope"),
    str_req("clientKey"),
    fh("clientSecret", FieldKind::StrRequired, HashMode::Always),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_req("type"),
    str_opt("url"),
];

// Note: `redactConfSecrets` only hashes the GitHub, OpenID Connect, GitLab,
// Bitbucket Cloud and Azure DevOps client secrets. Bitbucket Server's
// `clientSecret` is re-serialized but *not* hashed in the baseline (source of
// truth); kept as-is here for parity (errata, `internal/conf/validate.go:397-414`).
const BITBUCKETSERVER_FIELDS: &[FieldSpec] = &[
    fs("allowSignup", FieldKind::Bool),
    str_req("clientID"),
    str_req("clientSecret"),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_req("type"),
    str_opt("url"),
];

const BUILTIN_FIELDS: &[FieldSpec] = &[
    fs("allowSignup", FieldKind::Bool),
    str_req("type"),
];

const GERRIT_FIELDS: &[FieldSpec] = &[
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_req("type"),
    str_req("url"),
];

const GITHUB_FIELDS: &[FieldSpec] = &[
    fs("allowGroupsPermissionsSync", FieldKind::Bool),
    fs("allowOrgs", FieldKind::StrList),
    fs("allowOrgsMap", FieldKind::StrListMap),
    fs("allowSignup", FieldKind::Bool),
    str_req("clientID"),
    fh("clientSecret", FieldKind::StrRequired, HashMode::Always),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_req("type"),
    str_opt("url"),
];

const GITLAB_FIELDS: &[FieldSpec] = &[
    fs("allowGroups", FieldKind::StrList),
    fs("allowSignup", FieldKind::BoolPtr),
    str_opt("apiScope"),
    str_req("clientID"),
    fh("clientSecret", FieldKind::StrRequired, HashMode::Always),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_opt("ssoURL"),
    fs("syncInternalRepoPermissions", FieldKind::BoolPtr),
    fs("tokenRefreshWindowMinutes", FieldKind::Int),
    str_req("type"),
    str_opt("url"),
];

const HTTP_HEADER_FIELDS: &[FieldSpec] = &[
    str_opt("emailHeader"),
    str_opt("stripUsernameHeaderPrefix"),
    str_req("type"),
    str_req("usernameHeader"),
];

const OPENIDCONNECT_FIELDS: &[FieldSpec] = &[
    fs("allowSignup", FieldKind::BoolPtr),
    str_req("clientID"),
    fh("clientSecret", FieldKind::StrRequired, HashMode::Always),
    str_opt("configID"),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    fs("hidden", FieldKind::Bool),
    str_req("issuer"),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_opt("requireEmailDomain"),
    fs("singleIdentityPerUser", FieldKind::Bool),
    str_req("type"),
];

const SAML_FIELDS: &[FieldSpec] = &[
    fs("allowGroups", FieldKind::StrList),
    fs("allowSignup", FieldKind::BoolPtr),
    str_opt("configID"),
    str_opt("displayName"),
    str_ptr("displayPrefix"),
    str_opt("groupsAttributeName"),
    fs("hidden", FieldKind::Bool),
    str_opt("identityProviderMetadata"),
    str_opt("identityProviderMetadataURL"),
    fs(
        "insecureSkipAssertionSignatureValidation",
        FieldKind::Bool,
    ),
    str_opt("nameIDFormat"),
    fs("noSignIn", FieldKind::Bool),
    fs("order", FieldKind::Int),
    str_opt("serviceProviderCertificate"),
    str_opt("serviceProviderIssuer"),
    str_opt("serviceProviderPrivateKey"),
    fs("signRequests", FieldKind::BoolPtr),
    str_req("type"),
    fs("usernameAttributeNames", FieldKind::StrList),
];

const RESPONDER_FIELDS: &[FieldSpec] = &[
    str_opt("id"),
    str_opt("name"),
    str_opt("type"),
    str_opt("username"),
];

const NOTIFIER_SLACK_FIELDS: &[FieldSpec] = &[
    str_opt("icon_emoji"),
    str_opt("icon_url"),
    str_opt("recipient"),
    str_req("type"),
    fh("url", FieldKind::StrOptional, HashMode::IfNonEmpty),
    str_opt("username"),
];

const NOTIFIER_PAGERDUTY_FIELDS: &[FieldSpec] = &[
    str_opt("apiUrl"),
    fh(
        "integrationKey",
        FieldKind::StrRequired,
        HashMode::IfNonEmpty,
    ),
    str_opt("severity"),
    str_req("type"),
];

const NOTIFIER_WEBHOOK_FIELDS: &[FieldSpec] = &[
    fh("bearerToken", FieldKind::StrOptional, HashMode::IfNonEmpty),
    fh("password", FieldKind::StrOptional, HashMode::IfNonEmpty),
    str_req("type"),
    str_req("url"),
    str_opt("username"),
];

const NOTIFIER_EMAIL_FIELDS: &[FieldSpec] = &[str_req("address"), str_req("type")];

const NOTIFIER_OPSGENIE_FIELDS: &[FieldSpec] = &[
    fh("apiKey", FieldKind::StrOptional, HashMode::IfNonEmpty),
    str_opt("apiUrl"),
    str_opt("priority"),
    fs("responders", FieldKind::Responders),
    str_opt("tags"),
    str_req("type"),
];

const ALERT_DISABLE_FIELD: FieldSpec = fs("disableSendResolved", FieldKind::Bool);
const ALERT_LEVEL_FIELD: FieldSpec = str_req("level");
const ALERT_OWNERS_FIELD: FieldSpec = fs("owners", FieldKind::StrList);

fn invalid_type(field: &str) -> String {
    format!("parse config: json: cannot unmarshal value into Go struct field {field}")
}

fn encode_string(text: &str, hash: HashMode) -> String {
    match hash {
        HashMode::None => go_json_string(text),
        HashMode::Always => go_json_string(&redact_hash_string(text)),
        HashMode::IfNonEmpty => {
            if text.is_empty() {
                go_json_string(text)
            } else {
                go_json_string(&redact_hash_string(text))
            }
        }
    }
}

/// Encode one field of a schema struct the way `encoding/json` would:
/// required fields are always emitted, omitted-zero fields only when
/// non-zero, and pointer fields whenever they are present and non-null.
fn field_value(
    object: &BTreeMap<String, JsonValue>,
    spec: &FieldSpec,
) -> Result<Option<String>, String> {
    let value = object.get(spec.name);
    match spec.kind {
        FieldKind::StrRequired | FieldKind::StrOptional | FieldKind::StrPtr => {
            let text = match value {
                None | Some(JsonValue::Null) => match spec.kind {
                    FieldKind::StrRequired => Some(String::new()),
                    _ => None,
                },
                Some(JsonValue::String(text)) => match spec.kind {
                    FieldKind::StrRequired => Some(text.clone()),
                    FieldKind::StrOptional if text.is_empty() => None,
                    _ => Some(text.clone()),
                },
                Some(_) => return Err(invalid_type(spec.name)),
            };
            Ok(text.map(|text| encode_string(&text, spec.hash)))
        }
        FieldKind::Bool => match value {
            None | Some(JsonValue::Null) => Ok(None),
            Some(JsonValue::Bool(true)) => Ok(Some("true".to_owned())),
            Some(JsonValue::Bool(false)) => Ok(None),
            Some(_) => Err(invalid_type(spec.name)),
        },
        FieldKind::BoolPtr => match value {
            None | Some(JsonValue::Null) => Ok(None),
            Some(JsonValue::Bool(value)) => {
                Ok(Some(if *value { "true" } else { "false" }.to_owned()))
            }
            Some(_) => Err(invalid_type(spec.name)),
        },
        FieldKind::Int => match value {
            None | Some(JsonValue::Null) => Ok(None),
            Some(JsonValue::Number(lexeme)) => {
                let parsed: i64 = lexeme.parse().map_err(|_| invalid_type(spec.name))?;
                Ok(if parsed == 0 {
                    None
                } else {
                    Some(parsed.to_string())
                })
            }
            Some(_) => Err(invalid_type(spec.name)),
        },
        FieldKind::StrList => match value {
            None | Some(JsonValue::Null) => Ok(None),
            Some(JsonValue::Array(items)) => {
                let mut encoded = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::String(text) => encoded.push(go_json_string(text)),
                        _ => return Err(invalid_type(spec.name)),
                    }
                }
                Ok(if encoded.is_empty() {
                    None
                } else {
                    Some(format!("[{}]", encoded.join(",")))
                })
            }
            Some(_) => Err(invalid_type(spec.name)),
        },
        FieldKind::StrListMap => match value {
            None | Some(JsonValue::Null) => Ok(None),
            Some(JsonValue::Object(map)) => {
                let mut encoded = Vec::with_capacity(map.len());
                // `BTreeMap` iterates in sorted key order, like Go's map
                // marshaling.
                for (key, item) in map {
                    let JsonValue::Array(items) = item else {
                        return Err(invalid_type(spec.name));
                    };
                    let mut values = Vec::with_capacity(items.len());
                    for value in items {
                        match value {
                            JsonValue::String(text) => values.push(go_json_string(text)),
                            _ => return Err(invalid_type(spec.name)),
                        }
                    }
                    encoded.push(format!("{}:[{}]", go_json_string(key), values.join(",")));
                }
                Ok(if encoded.is_empty() {
                    None
                } else {
                    Some(format!("{{{}}}", encoded.join(",")))
                })
            }
            Some(_) => Err(invalid_type(spec.name)),
        },
        FieldKind::Responders => match value {
            None | Some(JsonValue::Null) => Ok(None),
            Some(JsonValue::Array(items)) => {
                let mut encoded = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::Null => encoded.push("null".to_owned()),
                        JsonValue::Object(object) => {
                            encoded.push(build_object(object, RESPONDER_FIELDS)?);
                        }
                        _ => return Err(invalid_type(spec.name)),
                    }
                }
                Ok(if encoded.is_empty() {
                    None
                } else {
                    Some(format!("[{}]", encoded.join(",")))
                })
            }
            Some(_) => Err(invalid_type(spec.name)),
        },
    }
}

fn build_object(
    object: &BTreeMap<String, JsonValue>,
    specs: &[FieldSpec],
) -> Result<String, String> {
    let mut parts = Vec::with_capacity(specs.len());
    for spec in specs {
        if let Some(value) = field_value(object, spec)? {
            parts.push(format!("{}:{}", go_json_string(spec.name), value));
        }
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

fn build_auth_provider(item: &JsonValue) -> Result<String, String> {
    let JsonValue::Object(object) = item else {
        return Err(invalid_type("SiteConfiguration.auth.providers"));
    };
    let discriminant = match object.get("type") {
        Some(JsonValue::String(value)) => value.as_str(),
        _ => return Err(invalid_type("AuthProviders.type")),
    };
    let specs = match discriminant {
        "azureDevOps" => AZURE_DEVOPS_FIELDS,
        "bitbucketcloud" => BITBUCKETCLOUD_FIELDS,
        "bitbucketserver" => BITBUCKETSERVER_FIELDS,
        "builtin" => BUILTIN_FIELDS,
        "gerrit" => GERRIT_FIELDS,
        "github" => GITHUB_FIELDS,
        "gitlab" => GITLAB_FIELDS,
        "http-header" => HTTP_HEADER_FIELDS,
        "openidconnect" => OPENIDCONNECT_FIELDS,
        "saml" => SAML_FIELDS,
        _ => return Err(invalid_type("AuthProviders.type")),
    };
    build_object(object, specs)
}

fn build_notifier(item: &JsonValue) -> Result<String, String> {
    let JsonValue::Object(object) = item else {
        return Err(invalid_type("Notifier"));
    };
    let discriminant = match object.get("type") {
        Some(JsonValue::String(value)) => value.as_str(),
        _ => return Err(invalid_type("Notifier.type")),
    };
    let specs = match discriminant {
        "slack" => NOTIFIER_SLACK_FIELDS,
        "pagerduty" => NOTIFIER_PAGERDUTY_FIELDS,
        "webhook" => NOTIFIER_WEBHOOK_FIELDS,
        "email" => NOTIFIER_EMAIL_FIELDS,
        "opsgenie" => NOTIFIER_OPSGENIE_FIELDS,
        _ => return Err(invalid_type("Notifier.type")),
    };
    build_object(object, specs)
}

fn build_alert(item: &JsonValue) -> Result<String, String> {
    let JsonValue::Object(object) = item else {
        return Err(invalid_type("ObservabilityAlerts"));
    };
    let mut parts = Vec::with_capacity(4);
    if let Some(value) = field_value(object, &ALERT_DISABLE_FIELD)? {
        parts.push(format!(
            "{}:{}",
            go_json_string(ALERT_DISABLE_FIELD.name),
            value
        ));
    }
    if let Some(value) = field_value(object, &ALERT_LEVEL_FIELD)? {
        parts.push(format!("{}:{}", go_json_string(ALERT_LEVEL_FIELD.name), value));
    }
    let notifier = object
        .get("notifier")
        .ok_or_else(|| invalid_type("ObservabilityAlerts.notifier"))?;
    parts.push(format!(
        "{}:{}",
        go_json_string("notifier"),
        build_notifier(notifier)?
    ));
    if let Some(value) = field_value(object, &ALERT_OWNERS_FIELD)? {
        parts.push(format!("{}:{}", go_json_string(ALERT_OWNERS_FIELD.name), value));
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

fn build_array(
    root: &JsonValue,
    path: &[&str],
    build: fn(&JsonValue) -> Result<String, String>,
) -> Result<Option<String>, String> {
    match value_at(root, path) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            if items.is_empty() {
                return Ok(None);
            }
            let mut encoded = Vec::with_capacity(items.len());
            for item in items {
                encoded.push(build(item)?);
            }
            Ok(Some(format!("[{}]", encoded.join(","))))
        }
        Some(_) => Err(invalid_type(path.last().copied().unwrap_or("array"))),
    }
}

/// `ParseConfig` unmarshals the whole site configuration, so a non-string
/// value at one of the `siteConfigSecrets` paths is a parse error. (The other
/// schema fields are not type-checked by this port; errata.)
fn validate_site_secret_types(root: &JsonValue) -> Result<(), String> {
    for path in SITE_CONFIG_SECRETS {
        if let Some(value) = value_at(root, path) {
            if !matches!(value, JsonValue::String(_) | JsonValue::Null) {
                return Err(invalid_type(&path.join(".")));
            }
        }
    }
    Ok(())
}

fn replace_property(output: &str, path: &[&str], json: String) -> Result<String, String> {
    let options = jsonc_format_options();
    let edits: Vec<Edit> = compute_property_edit(
        output,
        &property_path(path),
        EditValue::Json(json),
        &options,
    )?;
    apply_edits(output, &edits)
}

fn apply_edit(output: &mut String, path: Path, secret: &str) -> Result<(), String> {
    let options = jsonc_format_options();
    let value = EditValue::Json(go_json_string(&redact_hash_string(secret)));
    let edits: Vec<Edit> = compute_property_edit(output, &path, value, &options)?;
    *output = apply_edits(output, &edits)?;
    Ok(())
}

/// `conf.RedactAndHashSecrets` (`internal/conf/validate.go:371-479`): parse
/// the config, re-serialize `auth.providers` and `observability.alerts` with
/// their secrets hashed (this is what `ParseConfig` + `json.Marshal` does in
/// Go), hash the `siteConfigSecrets` paths, then run `jsonc.Format`.
pub fn redact_and_hash_secrets(site: &str) -> Result<String, String> {
    let value = parse_jsonc(site)?;
    validate_site_secret_types(&value)?;

    // Both arrays are built from the *original* document, before any edit.
    let auth_providers = build_array(&value, &["auth.providers"], build_auth_provider)?;
    let alerts = build_array(&value, &["observability.alerts"], build_alert)?;

    let mut output = site.to_owned();
    if let Some(providers) = auth_providers {
        output = replace_property(&output, &["auth.providers"], providers)?;
    }
    if let Some(alerts) = alerts {
        output = replace_property(&output, &["observability.alerts"], alerts)?;
    }

    // `siteConfigSecrets`: skip a property that is already empty; read the
    // raw value from the original document, skipping non-strings.
    for secret_path in SITE_CONFIG_SECRETS {
        // `jsonc.ReadProperty` ignores parse errors and the redacted document
        // is always valid here.
        let Ok(current) = parse_jsonc(&output) else {
            continue;
        };
        // `if ok && val == "" { continue }`.
        if string_at(&current, secret_path).as_deref() == Some("") {
            continue;
        }
        let Some(raw_secret) = string_at(&value, secret_path) else {
            continue;
        };
        apply_edit(&mut output, property_path(secret_path), &raw_secret)?;
    }

    let edits = format(&output, &jsonc_format_options());
    apply_edits(&output, &edits)
}

/// `ValidateSite`/`Validate`'s syntax stage only: the JSON Schema
/// (`schema.SiteSchemaJSON`), `validateCustomRaw` and `UnredactSecrets` are
/// not ported, so a document that parses as JSONC is accepted.
fn validate_site(site: &str) -> Result<(), String> {
    sg_config::SiteConfiguration::parse(site)
        .map(|_| ())
        .map_err(|err| format!("failed to validate site configuration: {err}"))
}

impl ConfigurationSource for DbConfigurationSource {
    async fn read(&self) -> Result<RawUnified, String> {
        // The ported read path has no actor context.
        self.read_with_author(0).await
    }

    async fn write(
        &self,
        data: RawUnified,
        last_id: i32,
        author_user_id: i32,
    ) -> Result<(), String> {
        self.write_with_override(data, last_id, author_user_id, false)
            .await
    }
}
