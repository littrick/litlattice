#![forbid(unsafe_code)]

//! Port of `conf.RedactSecrets` / `conf.UnredactSecrets`
//! (`internal/conf/validate.go:203-482`) for the `updateSiteConfiguration`
//! write path.
//!
//! The baseline round-trips `auth.providers` and `observability.alerts`
//! through typed Go structs (`json.Marshal` of the parsed values), while this
//! port edits the *raw* JSON values in place and preserves the rest of the
//! document. For valid, canonical inputs the resulting JSON is semantically
//! identical (required provider fields are present in the input); key order,
//! `omitempty` defaults for absent fields and unknown fields can differ. See
//! `docs/overviews/m9_8gn_update_site_configuration_overview.md` §4.2.
//!
//! The literal top-level keys (for example `executors.accessToken`) are kept
//! verbatim: they are one property name, not a nested path.

use serde_json::Value;
use sg_config::RawUnified;
use sg_jsonx::{
    apply_edits, compute_property_edit, format, go_json_string, settings_format_options, EditValue,
    Path, Segment,
};
use sha2::{Digest, Sha256};

/// `conf.redactedSecret`.
pub const REDACTED_SECRET: &str = "REDACTED";

/// One `siteConfigSecrets` entry. The read path in Go uses gjson's escaped
/// dotted form; the edit path is the list of literal property names.
struct SecretPath {
    edit_paths: &'static [&'static str],
}

const SITE_CONFIG_SECRETS: &[SecretPath] = &[
    SecretPath {
        edit_paths: &["executors.accessToken"],
    },
    SecretPath {
        edit_paths: &["email.smtp", "username"],
    },
    SecretPath {
        edit_paths: &["email.smtp", "password"],
    },
    SecretPath {
        edit_paths: &["organizationInvitations", "signingKey"],
    },
    SecretPath {
        edit_paths: &["githubClientSecret"],
    },
    SecretPath {
        edit_paths: &["dotcom", "githubApp.cloud", "clientSecret"],
    },
    SecretPath {
        edit_paths: &["dotcom", "githubApp.cloud", "privateKey"],
    },
    SecretPath {
        edit_paths: &["gitHubApp", "privateKey"],
    },
    SecretPath {
        edit_paths: &["gitHubApp", "clientSecret"],
    },
    SecretPath {
        edit_paths: &["auth.unlockAccountLinkSigningKey"],
    },
    SecretPath {
        edit_paths: &["dotcom", "srcCliVersionCache", "github", "token"],
    },
    SecretPath {
        edit_paths: &["dotcom", "srcCliVersionCache", "github", "webhookSecret"],
    },
    SecretPath {
        edit_paths: &["dotcom", "sams.clientSecret"],
    },
    SecretPath {
        edit_paths: &["embeddings", "accessToken"],
    },
    SecretPath {
        edit_paths: &["completions", "accessToken"],
    },
    SecretPath {
        edit_paths: &["app", "dotcomAuthToken"],
    },
    SecretPath {
        edit_paths: &["attribution.gateway", "accessToken"],
    },
    SecretPath {
        edit_paths: &["scim.authToken"],
    },
];

/// The auth provider types whose `clientSecret` participates in
/// unredaction, and the property holding the old-secret lookup key
/// (`validate.go:239-256`).
const AUTH_PROVIDER_CLIENT_KEYS: &[(&str, &str)] = &[
    ("openidconnect", "clientID"),
    ("github", "clientID"),
    ("gitlab", "clientID"),
    ("bitbucketcloud", "clientKey"),
    ("azureDevOps", "clientID"),
];

/// `conf.redactHashString`: `REDACTED-DATA-CHUNK-` + the first ten hex digits
/// of the SHA-256 digest.
pub fn redact_hash_string(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("REDACTED-DATA-CHUNK-{}", &hex[..10])
}

/// `conf.UnredactSecrets(input, raw)`: restore `REDACTED` placeholders (and
/// hashed observability secrets) from the previous raw configuration.
pub fn unredact_secrets(input: &str, raw: &RawUnified) -> Result<String, String> {
    let old = sg_config::SiteConfiguration::parse(&raw.site)
        .map_err(|err| format!("parse old config: {err}"))?;
    let new = sg_config::SiteConfiguration::parse(input)
        .map_err(|err| format!("parse new config: {err}"))?;

    // Old provider secrets, keyed by the provider's client id/key.
    let mut old_secrets: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    if let Some(providers) = providers(old.raw()) {
        for provider in providers {
            if let Some((_, client_key)) = provider_type(provider) {
                let key = provider
                    .get(client_key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let secret = provider
                    .get("clientSecret")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                old_secrets.insert(key, secret);
            }
        }
    }

    // Restore the `REDACTED` client secrets in the new providers.
    let providers_value = match providers(new.raw()).map(|providers| {
        let mut restored = Vec::with_capacity(providers.len());
        for provider in providers {
            let mut provider = provider.clone();
            if let Some((_, client_key)) = provider_type(&provider) {
                if provider.get("clientSecret").and_then(Value::as_str) == Some(REDACTED_SECRET) {
                    let key = provider
                        .get(client_key)
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let secret = old_secrets.get(key).cloned().unwrap_or_default();
                    provider
                        .as_object_mut()
                        .expect("providers are objects")
                        .insert("clientSecret".to_owned(), Value::String(secret));
                }
            }
            restored.push(provider);
        }
        Value::Array(restored)
    }) {
        Some(value) => value,
        // `auth.providers` absent/null marshals a nil slice to `null`.
        None => Value::Null,
    };

    let mut text = edit_json_value(input, &["auth.providers"], &providers_value)
        .map_err(|err| format!("unredact \"auth.providers\": {err}"))?;

    // Hashed observability notifier secrets in the old config.
    let mut replacements: Vec<(String, String)> = Vec::new();
    if let Some(alerts) = array(old.raw().get("observability.alerts")) {
        for alert in alerts {
            let Some(notifier) = alert.get("notifier") else {
                continue;
            };
            for key in notifier_secret_keys(notifier) {
                let Some(secret) = notifier.get(*key).and_then(Value::as_str) else {
                    continue;
                };
                if secret.is_empty() {
                    continue;
                }
                replacements.push((redact_hash_string(secret), secret.to_owned()));
            }
        }
    }
    text = replace_once(&text, &replacements);

    // Restore unchanged `REDACTED` secrets from the raw document.
    for secret in SITE_CONFIG_SECRETS {
        let Some(current) = read_property(&text, secret.edit_paths) else {
            continue;
        };
        if let Value::String(value) = &current {
            if value != REDACTED_SECRET {
                continue;
            }
        }

        let Some(Value::String(previous)) = read_property(&raw.site, secret.edit_paths) else {
            continue;
        };

        text = edit_string(&text, secret.edit_paths, &previous)
            .map_err(|err| format!("unredact {:?}: {err}", secret.edit_paths.join(" > ")))?;
    }

    format_document(&text).map_err(|err| format!("JSON formatting: {err}"))
}

/// `conf.RedactSecrets(raw)`: the formatted redacted site config, or the empty
/// string when parsing/redaction fails (the baseline's empty `RawUnified`).
pub fn redact_secrets(raw: &RawUnified) -> String {
    redact_conf_secrets(&raw.site).unwrap_or_default()
}

fn redact_conf_secrets(site: &str) -> Result<String, String> {
    let config =
        sg_config::SiteConfiguration::parse(site).map_err(|err| format!("parse config: {err}"))?;
    let raw = config.raw();

    // Auth providers: `clientSecret` is replaced even when absent, because the
    // typed Go field has no `omitempty`.
    let mut redacted = site.to_owned();
    if let Some(providers) = providers(raw) {
        if !providers.is_empty() {
            let mut value = Vec::with_capacity(providers.len());
            for provider in providers {
                let mut provider = provider.clone();
                if provider_type(&provider).is_some() {
                    provider
                        .as_object_mut()
                        .expect("providers are objects")
                        .insert(
                            "clientSecret".to_owned(),
                            Value::String(REDACTED_SECRET.to_owned()),
                        );
                }
                value.push(provider);
            }
            redacted = edit_json_value(&redacted, &["auth.providers"], &Value::Array(value))
                .map_err(|err| format!("redact \"auth.providers\": {err}"))?;
        }
    }

    // Observability notifier secrets are always hashed.
    if let Some(alerts) = array(raw.get("observability.alerts")) {
        if !alerts.is_empty() {
            let mut value = Vec::with_capacity(alerts.len());
            for alert in alerts {
                let mut alert = alert.clone();
                if let Some(notifier) = alert.get_mut("notifier").and_then(Value::as_object_mut) {
                    // The notifier types are a tagged union; only the secrets
                    // of the selected type exist in the typed Go config.
                    let kind = notifier.get("type").and_then(Value::as_str);
                    for key in notifier_secret_keys_for(kind) {
                        if let Some(Value::String(secret)) = notifier.get_mut(*key) {
                            if !secret.is_empty() {
                                *secret = redact_hash_string(secret);
                            }
                        }
                    }
                }
                value.push(alert);
            }
            redacted = edit_json_value(&redacted, &["observability.alerts"], &Value::Array(value))
                .map_err(|err| format!("redact \"observability.alerts\": {err}"))?;
        }
    }

    for secret in SITE_CONFIG_SECRETS {
        let Some(current) = read_property(&redacted, secret.edit_paths) else {
            continue;
        };
        if let Value::String(value) = &current {
            if value.is_empty() {
                continue;
            }
        }

        let Some(Value::String(previous)) = read_value(raw, secret.edit_paths) else {
            continue;
        };

        // `RedactSecrets` (not `RedactAndHashSecrets`) always substitutes the
        // literal placeholder.
        redacted = edit_string(&redacted, secret.edit_paths, REDACTED_SECRET)
            .map_err(|err| format!("redact {:?}: {err}", secret.edit_paths.join(" > ")))?;
        let _ = previous;
    }

    format_document(&redacted).map_err(|err| format!("JSON formatting: {err}"))
}

fn providers(raw: &Value) -> Option<&Vec<Value>> {
    raw.get("auth.providers").and_then(Value::as_array)
}

fn provider_type(provider: &Value) -> Option<(&'static str, &'static str)> {
    let kind = provider.get("type").and_then(Value::as_str)?;
    AUTH_PROVIDER_CLIENT_KEYS
        .iter()
        .copied()
        .find(|(name, _)| *name == kind)
}

fn array(value: Option<&Value>) -> Option<&Vec<Value>> {
    value.and_then(Value::as_array)
}

/// The secret properties of an observability notifier, selected by the
/// notifier's tagged-union `type` (`schema.Notifier*`).
fn notifier_secret_keys(notifier: &Value) -> &'static [&'static str] {
    notifier_secret_keys_for(notifier.get("type").and_then(Value::as_str))
}

fn notifier_secret_keys_for(kind: Option<&str>) -> &'static [&'static str] {
    match kind {
        Some("slack") => &["url"],
        Some("pagerduty") => &["integrationKey"],
        Some("webhook") => &["bearerToken", "password"],
        Some("opsgenie") => &["apiKey"],
        _ => &[],
    }
}

/// Read a literal property path from a JSONC document. Missing properties and
/// unparseable documents yield `None` (Go's `jsonc.ReadProperty` error).
fn read_property(text: &str, path: &[&str]) -> Option<Value> {
    read_value(&sg_config::parse_jsonc(text).ok()?, path)
}

fn read_value(value: &Value, path: &[&str]) -> Option<Value> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current.clone())
}

fn path_of(segments: &[&str]) -> Path {
    segments
        .iter()
        .map(|segment| Segment::Property((*segment).to_owned()))
        .collect()
}

fn edit_raw(text: &str, segments: &[&str], value: EditValue) -> Result<String, String> {
    let options = settings_format_options();
    let edits = compute_property_edit(text, &path_of(segments), value, &options)?;
    apply_edits(text, &edits)
}

fn edit_json_value(text: &str, segments: &[&str], value: &Value) -> Result<String, String> {
    let encoded = serde_json::to_string(value).map_err(|err| err.to_string())?;
    edit_raw(text, segments, EditValue::Json(encoded))
}

fn edit_string(text: &str, segments: &[&str], value: &str) -> Result<String, String> {
    edit_raw(text, segments, EditValue::Json(go_json_string(value)))
}

fn format_document(text: &str) -> Result<String, String> {
    let options = settings_format_options();
    let edits = format(text, &options);
    apply_edits(text, &edits)
}

/// `strings.NewReplacer(pairs...).Replace`: a single left-to-right pass; at
/// each position the first matching pair (in argument order) wins and its
/// replacement is not rescanned.
fn replace_once(text: &str, pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    'scan: while index < text.len() {
        for (old, new) in pairs {
            if text[index..].starts_with(old) {
                out.push_str(new);
                index += old.len();
                continue 'scan;
            }
        }
        let ch = text[index..]
            .chars()
            .next()
            .expect("index is a char boundary");
        out.push(ch);
        index += ch.len_utf8();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_config::ServiceConnections;

    fn raw(site: &str) -> RawUnified {
        RawUnified {
            id: 1,
            site: site.to_owned(),
            service_connections: ServiceConnections::default(),
        }
    }

    #[test]
    fn hash_format_matches_the_baseline() {
        assert_eq!(
            redact_hash_string("secret"),
            "REDACTED-DATA-CHUNK-2bb80d537b"
        );
    }

    #[test]
    fn unredacts_auth_provider_secrets() {
        let old = raw(
            r#"{"auth.providers":[{"type":"github","clientID":"cid","clientSecret":"old-secret"}]}"#,
        );
        let input = r#"{"externalURL":"https://example.com","auth.providers":[{"type":"github","clientID":"cid","clientSecret":"REDACTED"}]}"#;
        let out = unredact_secrets(input, &old).expect("unredact");
        let value: Value = sg_config::parse_jsonc(&out).expect("json");
        assert_eq!(
            value["auth.providers"][0]["clientSecret"],
            Value::String("old-secret".to_owned())
        );
        assert_eq!(value["externalURL"], "https://example.com");
    }

    #[test]
    fn unredacts_literal_dotted_keys() {
        let old = raw(r#"{"scim.authToken":"token","email.smtp":{"username":"u"}}"#);
        let input = r#"{"scim.authToken":"REDACTED","email.smtp":{"username":"REDACTED"}}"#;
        let out = unredact_secrets(input, &old).expect("unredact");
        let value: Value = sg_config::parse_jsonc(&out).expect("json");
        assert_eq!(value["scim.authToken"], "token");
        assert_eq!(value["email.smtp"]["username"], "u");
    }

    #[test]
    fn missing_auth_providers_become_null() {
        let old = raw("{}");
        let out =
            unredact_secrets(r#"{"externalURL":"https://example.com"}"#, &old).expect("unredact");
        let value: Value = sg_config::parse_jsonc(&out).expect("json");
        assert_eq!(value["auth.providers"], Value::Null);
    }

    #[test]
    fn redacts_auth_providers_and_observability_secrets() {
        let config = raw(
            r#"{"auth.providers":[{"type":"github","clientID":"cid","clientSecret":"top"}],
                "observability.alerts":[{"notifier":{"type":"slack","url":"https://hooks/secret"}}]}"#,
        );
        let redacted = redact_secrets(&config);
        let value: Value = sg_config::parse_jsonc(&redacted).expect("json");
        assert_eq!(value["auth.providers"][0]["clientSecret"], REDACTED_SECRET);
        assert_eq!(
            value["observability.alerts"][0]["notifier"]["url"],
            redact_hash_string("https://hooks/secret")
        );
    }

    #[test]
    fn redaction_failure_yields_the_empty_string() {
        assert_eq!(redact_secrets(&raw("{")), "");
    }

    #[test]
    fn replacement_does_not_rescan_the_output() {
        let pairs = vec![
            ("a".to_owned(), "b".to_owned()),
            ("b".to_owned(), "c".to_owned()),
        ];
        assert_eq!(replace_once("ab", &pairs), "bc");
    }
}
