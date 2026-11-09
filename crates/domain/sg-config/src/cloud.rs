//! Port of `internal/cloud`: the signed Sourcegraph Cloud site configuration
//! read from `SRC_CLOUD_SITE_CONFIG`.
//!
//! The environment variable holds a base64 (raw URL alphabet, no padding)
//! encoded JSON document whose `siteConfig` field is the base64 (standard
//! alphabet) encoded site configuration and whose `signature` field is the
//! SSH ed25519 signature (format `ssh-ed25519`) over the decoded site
//! configuration bytes. The signer's public key is baked into the binary, so
//! only Sourcegraph Cloud can produce an accepted document.
//!
//! `cloud.SiteConfig` parses lazily and **panics** on a malformed document;
//! the worker startup path is the only consumer in this workspace, and it
//! turns the parse failure into a startup error instead (the process still
//! refuses to start, which is the observable behaviour that matters — see
//! `docs/modules/06_worker.md` §33).
//!
//! The `siteConfigAllowlist` error-message template (`text/template`) is
//! parsed by the baseline only to render GraphQL validation errors; it is
//! carried through the JSON but not compiled here.

use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine as _;
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::Deserialize;

/// `SRC_CLOUD_SITE_CONFIG`: the base64-encoded signed cloud site config.
pub const RAW_SITE_CONFIG_ENV: &str = "SRC_CLOUD_SITE_CONFIG";

/// `sourcegraphCloudSiteConfigSignerPublicKey`, verbatim from
/// `internal/cloud/site_config.go`.
pub const SOURCEGRAPH_CLOUD_SITE_CONFIG_SIGNER_PUBLIC_KEY: &str =
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFnVjzARMu+jaSrTgvJCpWEDP503Y3k3DMbs5ghHOkML";

/// `cloud.SchemaSiteConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct SchemaSiteConfig {
    #[serde(rename = "authProviders", alias = "AuthProviders")]
    pub auth_providers: Option<SchemaAuthProviders>,
    #[serde(rename = "siteConfigAllowlist", alias = "SiteConfigAllowlist")]
    pub site_config_allowlist: SiteConfigAllowlistSpec,
}

impl SchemaSiteConfig {
    /// `SchemaSiteConfig.SourcegraphOperatorAuthProviderEnabled`.
    pub fn sourcegraph_operator_auth_provider_enabled(&self) -> bool {
        self.auth_providers
            .as_ref()
            .is_some_and(|providers| providers.sourcegraph_operator.is_some())
    }

    /// `SchemaSiteConfig.SiteConfigAllowlistEnabled`.
    pub fn site_config_allowlist_enabled(&self) -> bool {
        self.sourcegraph_operator_auth_provider_enabled()
            && !self.site_config_allowlist.paths.is_empty()
    }
}

/// `cloud.SiteConfigAllowlistSpec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct SiteConfigAllowlistSpec {
    #[serde(rename = "notAllowedErrorMessage", alias = "NotAllowedErrorMessage")]
    pub not_allowed_error_message: String,
    #[serde(rename = "paths", alias = "Paths")]
    pub paths: Vec<String>,
}

/// `cloud.SchemaAuthProviders`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct SchemaAuthProviders {
    #[serde(rename = "sourcegraphOperator", alias = "SourcegraphOperator")]
    pub sourcegraph_operator: Option<SchemaAuthProviderSourcegraphOperator>,
}

/// `cloud.SchemaAuthProviderSourcegraphOperator`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct SchemaAuthProviderSourcegraphOperator {
    #[serde(rename = "issuer", alias = "Issuer")]
    pub issuer: String,
    #[serde(rename = "clientID", alias = "ClientID")]
    pub client_id: String,
    #[serde(rename = "clientSecret", alias = "ClientSecret")]
    pub client_secret: String,
    /// `LifecycleDuration` indicates duration in minutes before accounts
    /// created through SOAP are expired and removed.
    #[serde(rename = "lifecycleDuration", alias = "LifecycleDuration")]
    pub lifecycle_duration: i64,
}

/// `sourcegraphoperator.LifecycleDuration`: a non-positive value means the
/// 60-minute default.
pub fn lifecycle_duration(minutes: i64) -> Duration {
    if minutes <= 0 {
        Duration::from_secs(60 * 60)
    } else {
        Duration::from_secs((minutes as u64).saturating_mul(60))
    }
}

/// `cloud.SignedSiteConfig`.
#[derive(Debug, Deserialize)]
struct SignedSiteConfig {
    #[serde(rename = "signature", alias = "Signature", default)]
    signature: Option<SshSignature>,
    /// `[]byte` in Go: the JSON value is base64 (standard alphabet).
    #[serde(rename = "siteConfig", alias = "SiteConfig")]
    site_config: String,
}

/// `golang.org/x/crypto/ssh.Signature` as serialized by `encoding/json`.
#[derive(Debug, Deserialize)]
struct SshSignature {
    #[serde(rename = "Format", alias = "format")]
    format: String,
    #[serde(rename = "Blob", alias = "blob")]
    blob: String,
}

static SITE_CONFIG: OnceLock<Result<SchemaSiteConfig, String>> = OnceLock::new();

/// `cloud.SiteConfig()`: parse `SRC_CLOUD_SITE_CONFIG` once. An unset or empty
/// variable yields the stub document, which disables every cloud-only
/// feature. A malformed document is reported once and cached.
pub fn site_config() -> Result<&'static SchemaSiteConfig, &'static str> {
    SITE_CONFIG
        .get_or_init(|| match std::env::var(RAW_SITE_CONFIG_ENV) {
            Ok(raw) if !raw.is_empty() => parse_site_config(&raw),
            _ => Ok(SchemaSiteConfig::default()),
        })
        .as_ref()
        .map_err(String::as_str)
}

/// `cloud.parseSiteConfig` step for step, with the baseline's error texts.
pub fn parse_site_config(raw: &str) -> Result<SchemaSiteConfig, String> {
    parse_site_config_with_public_key(raw, SOURCEGRAPH_CLOUD_SITE_CONFIG_SIGNER_PUBLIC_KEY)
}

fn parse_site_config_with_public_key(
    raw: &str,
    authorized_key: &str,
) -> Result<SchemaSiteConfig, String> {
    let public_key = ssh_ed25519_public_key(authorized_key)
        .map_err(|err| format!("parse signer public key: {err}"))?;

    // `base64.RawURLEncoding.DecodeString`.
    let signed_data = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|err| format!("decode raw site config: {err}"))?;

    let signed: SignedSiteConfig = serde_json::from_slice(&signed_data)
        .map_err(|err| format!("unmarshal signed data: {err}"))?;

    let signature = signed
        .signature
        .ok_or_else(|| "verify signed data: missing signature".to_owned())?;
    if signature.format != "ssh-ed25519" {
        return Err(format!(
            "verify signed data: unsupported signature format {:?}",
            signature.format
        ));
    }
    let signature_blob = base64::engine::general_purpose::STANDARD
        .decode(&signature.blob)
        .map_err(|err| format!("verify signed data: {err}"))?;

    // `[]byte` JSON fields use the standard base64 alphabet.
    let site_config_bytes = base64::engine::general_purpose::STANDARD
        .decode(&signed.site_config)
        .map_err(|err| format!("unmarshal signed data: {err}"))?;

    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(&site_config_bytes, &signature_blob)
        .map_err(|_| "verify signed data: signature verification failed".to_owned())?;

    serde_json::from_slice(&site_config_bytes)
        .map_err(|err| format!("unmarshal verified site config: {err}"))
}

/// Parse an OpenSSH `ssh-ed25519` authorized-keys line into the raw 32-byte
/// public key (`crypto/ssh.ParseAuthorizedKey`).
fn ssh_ed25519_public_key(authorized_key: &str) -> Result<[u8; 32], String> {
    let mut fields = authorized_key.split_whitespace();
    let key_type = fields.next().ok_or_else(|| "no key type".to_owned())?;
    let encoded = fields.next().ok_or_else(|| "no key data".to_owned())?;
    if key_type != "ssh-ed25519" {
        return Err(format!("unsupported key type {key_type:?}"));
    }
    let blob = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|err| err.to_string())?;

    let mut offset = 0;
    let algo = read_ssh_string(&blob, &mut offset)?;
    if algo != b"ssh-ed25519" {
        return Err("algorithm name mismatch".to_owned());
    }
    let key = read_ssh_string(&blob, &mut offset)?;
    if key.len() != 32 {
        return Err(format!("unexpected ed25519 key length {}", key.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(key);
    Ok(out)
}

fn read_ssh_string<'a>(blob: &'a [u8], offset: &mut usize) -> Result<&'a [u8], String> {
    let header = blob
        .get(*offset..*offset + 4)
        .ok_or_else(|| "truncated key blob".to_owned())?;
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    *offset += 4;
    let value = blob
        .get(*offset..*offset + length)
        .ok_or_else(|| "truncated key blob".to_owned())?;
    *offset += length;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn signed_document(site_config: &str, key_pair: &Ed25519KeyPair) -> String {
        let signature = key_pair.sign(site_config.as_bytes());
        let document = serde_json::json!({
            "signature": {
                "Format": "ssh-ed25519",
                "Rest": null,
                "Blob": base64::engine::general_purpose::STANDARD.encode(signature.as_ref()),
            },
            "siteConfig": base64::engine::general_purpose::STANDARD.encode(site_config.as_bytes()),
        });
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&document).expect("json"))
    }

    fn authorized_key(key_pair: &Ed25519KeyPair) -> String {
        let public_key = key_pair.public_key().as_ref();
        let mut blob = Vec::new();
        blob.extend_from_slice(&(b"ssh-ed25519".len() as u32).to_be_bytes());
        blob.extend_from_slice(b"ssh-ed25519");
        blob.extend_from_slice(&(public_key.len() as u32).to_be_bytes());
        blob.extend_from_slice(public_key);
        format!(
            "ssh-ed25519 {}",
            base64::engine::general_purpose::STANDARD.encode(blob)
        )
    }

    fn key_pair() -> Ed25519KeyPair {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("keypair");
        Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("pkcs8")
    }

    #[test]
    fn verifies_a_signed_cloud_config() {
        let pair = key_pair();
        let raw = signed_document(
            r#"{"authProviders":{"sourcegraphOperator":{"issuer":"https://example.com","clientID":"id","clientSecret":"secret","lifecycleDuration":120}}}"#,
            &pair,
        );
        let parsed = parse_site_config_with_public_key(&raw, &authorized_key(&pair))
            .expect("verified config");
        assert!(parsed.sourcegraph_operator_auth_provider_enabled());
        let operator = parsed.auth_providers.unwrap().sourcegraph_operator.unwrap();
        assert_eq!(operator.issuer, "https://example.com");
        assert_eq!(operator.client_id, "id");
        assert_eq!(operator.client_secret, "secret");
        assert_eq!(operator.lifecycle_duration, 120);
    }

    #[test]
    fn rejects_a_tampered_site_config() {
        let pair = key_pair();
        let raw = signed_document(r#"{"authProviders":{}}"#, &pair);
        // Flip one character of the raw URL-safe base64 document so the
        // embedded site config no longer matches its signature.
        let mut bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&raw)
            .expect("decode");
        let position = bytes.len() / 2;
        bytes[position] ^= 1;
        let tampered = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let err = parse_site_config_with_public_key(&tampered, &authorized_key(&pair))
            .expect_err("tampered config");
        assert!(
            err.starts_with("decode raw site config:")
                || err.starts_with("unmarshal signed data:")
                || err.starts_with("verify signed data:"),
            "{err}"
        );
    }

    #[test]
    fn rejects_a_signature_from_another_key() {
        let signer = key_pair();
        let verifier = key_pair();
        let raw = signed_document(r#"{"authProviders":{}}"#, &signer);
        let err = parse_site_config_with_public_key(&raw, &authorized_key(&verifier))
            .expect_err("wrong signer");
        assert_eq!(err, "verify signed data: signature verification failed");
    }

    #[test]
    fn empty_environment_is_the_disabled_stub() {
        // Mirrors `SiteConfig()` when `SRC_CLOUD_SITE_CONFIG` is empty.
        let stub = SchemaSiteConfig::default();
        assert!(!stub.sourcegraph_operator_auth_provider_enabled());
        assert!(!stub.site_config_allowlist_enabled());
    }

    #[test]
    fn lifecycle_duration_defaults_to_one_hour_for_non_positive_minutes() {
        assert_eq!(lifecycle_duration(0), Duration::from_secs(3600));
        assert_eq!(lifecycle_duration(-5), Duration::from_secs(3600));
        assert_eq!(lifecycle_duration(1), Duration::from_secs(60));
        assert_eq!(lifecycle_duration(90), Duration::from_secs(5400));
    }

    #[test]
    fn allowlist_enabled_requires_paths_and_operator_provider() {
        let mut config = SchemaSiteConfig {
            auth_providers: Some(SchemaAuthProviders {
                sourcegraph_operator: Some(SchemaAuthProviderSourcegraphOperator::default()),
            }),
            site_config_allowlist: SiteConfigAllowlistSpec::default(),
        };
        assert!(!config.site_config_allowlist_enabled());
        config.site_config_allowlist.paths = vec!["auth.providers".to_owned()];
        assert!(config.site_config_allowlist_enabled());
    }
}
