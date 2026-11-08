#![forbid(unsafe_code)]

//! Port of the parts of `internal/license` and `internal/licensing` that the
//! repo-updater syncer needs:
//!
//! - [`Info`]: the claims embedded in a Sourcegraph product license key
//!   (`internal/license/license.go` `Info`).
//! - [`parse_signed_key`]: decode the base64url-encoded, SSH-RSA-signed
//!   license key and verify its signature (`license.ParseSignedKey`).
//! - [`check_private_repositories`]: the effective private-repository limit
//!   for the configured license (`licensing.Check` +
//!   `FeaturePrivateRepositories.Check`), including the free plan that applies
//!   when no license key is configured.
//!
//! Divergences from the baseline, both documented in `docs`:
//!
//! - The license generation key override
//!   (`SOURCEGRAPH_LICENSE_GENERATION_KEY`) is not supported: verification
//!   always uses the hardcoded builtin key, so development licenses signed
//!   with a generation key are rejected (the baseline also rejects them in
//!   `ParseProductLicenseKey`, which the syncer's hooks use).
//! - `licensing.IsLicenseValid()` reads a Redis validity flag set by the
//!   frontend; without access to that Redis key every license is treated as
//!   valid (the baseline's default when the key is absent).

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::signature::Verifier;
use rsa::RsaPublicKey;
use serde::Deserialize;
use sha1::Sha1;

/// `licensing.publicKeyData`: the public key used to verify product license
/// keys. Copied verbatim from `internal/licensing/licensing.go`.
pub const PUBLIC_KEY_DATA: &str = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDUUd9r83fGmYVLzcqQp5InyAoJB5lLxlM7s41SUUtxfnG6JpmvjNd+WuEptJGk0C/Zpyp/cCjCV4DljDs8Z7xjRbvJYW+vklFFxXrMTBs/+HjpIBKlYTmG8SqTyXyu1s4485Kh1fEC5SK6z2IbFaHuSHUXgDi/IepSOg1QudW4n8J91gPtT2E30/bPCBRq8oz/RVwJSDMvYYjYVb//LhV0Mx3O6hg4xzUNuwiCtNjCJ9t4YU2sV87+eJwWtQNbSQ8TelQa8WjG++XSnXUHw12bPDe7wGL/7/EJb7knggKSAMnpYpCyV35dyi4DsVc46c+b6P0gbVSosh3Uc3BJHSWF";

/// `license.Info`: the signed (but not encrypted) claims in a license key.
/// Only the tags drive feature gating; the other fields are kept for
/// completeness.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Info {
    /// `t`: tags denote features/restrictions (e.g. `plan:free-1`).
    #[serde(default, rename = "t")]
    pub tags: Vec<String>,
    /// `u`: the number of users the license is valid for.
    #[serde(default, rename = "u")]
    pub user_count: u64,
    /// `c`: creation timestamp (RFC 3339). Zero for version < 3 licenses.
    #[serde(default, rename = "c")]
    pub created_at: Option<String>,
    /// `e`: expiry timestamp (RFC 3339).
    #[serde(default, rename = "e")]
    pub expires_at: Option<String>,
    #[serde(default, rename = "sf_sub_id")]
    pub salesforce_subscription_id: Option<String>,
    #[serde(default, rename = "sf_opp_id")]
    pub salesforce_opportunity_id: Option<String>,
}

impl Info {
    /// `Info.HasTag`: tag comparison ignores surrounding whitespace, because
    /// historically the license web form submitted tags with spaces.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t.trim() == tag)
    }

    /// `Plan.IsFreePlan`: the `free-0` / `free-1` plans.
    pub fn is_free_plan(&self) -> bool {
        matches!(self.plan(), Plan::Free0 | Plan::Free1)
    }

    /// `BasicFeature("allow-air-gapped").Check(info)`: the plan includes the
    /// feature, or the license carries the explicit tag.
    pub fn allows_air_gapped(&self) -> bool {
        self.plan() == Plan::AirGappedEnterprise || self.has_tag("allow-air-gapped")
    }

    /// `PlanFromTags`: the first known `plan:` tag, defaulting to the old
    /// enterprise plan when no plan tag is present.
    pub fn plan(&self) -> Plan {
        for tag in &self.tags {
            if let Some(rest) = tag.strip_prefix(PLAN_TAG_PREFIX) {
                let plan = Plan::from_str(rest);
                if plan.is_known() {
                    return plan;
                }
            }
        }
        Plan::OldEnterprise
    }

    /// `Info.HasUnknownPlan`: a `plan:` tag naming an unrecognised plan is an
    /// error.
    pub fn unknown_plan_tag(&self) -> Option<&str> {
        self.tags.iter().find_map(|tag| {
            let rest = tag.strip_prefix(PLAN_TAG_PREFIX)?;
            if Plan::from_str(rest).is_known() {
                None
            } else {
                Some(tag.as_str())
            }
        })
    }
}

const PLAN_TAG_PREFIX: &str = "plan:";

/// The plans defined in `internal/licensing/data.go`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    OldEnterprise,
    Team0,
    Enterprise0,
    Enterprise1,
    Free0,
    Free1,
    AirGappedEnterprise,
    CodyEnterprise,
    CodeIntelligencePlatform,
    /// A `plan:` tag that names no known plan.
    Unknown,
}

impl Plan {
    fn from_str(plan: &str) -> Self {
        match plan {
            "old-enterprise-0" => Self::OldEnterprise,
            "team-0" => Self::Team0,
            "enterprise-0" => Self::Enterprise0,
            "enterprise-1" => Self::Enterprise1,
            "free-0" => Self::Free0,
            "free-1" => Self::Free1,
            "enterprise-air-gap-0" => Self::AirGappedEnterprise,
            "cody-enterprise-0" => Self::CodyEnterprise,
            "code-ai-enterprise-0" => Self::CodeIntelligencePlatform,
            _ => Self::Unknown,
        }
    }

    fn is_known(self) -> bool {
        self != Self::Unknown
    }
}

/// `FeaturePrivateRepositories` as resolved against a license.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateRepositories {
    /// No limit on private repositories.
    Unrestricted,
    /// At most `n` private repositories; `CreateRepoLicenseHook` rejects the
    /// `(n+1)`th and `UpdateRepoLicenseHook` rejects the `(n+1)`th private
    /// repo (public→private or revive).
    Limited(usize),
    /// The license does not include the feature.
    NotActivated,
}

impl PrivateRepositories {
    /// The error text the hooks return for this feature state, matching
    /// `internal/repos/syncer.go`.
    pub fn error_message(self) -> String {
        match self {
            PrivateRepositories::Unrestricted => String::new(),
            PrivateRepositories::Limited(max) => format!(
                "maximum number of private repositories included in license ({max}) reached"
            ),
            PrivateRepositories::NotActivated => "The private repositories feature is not \
                 activated for this license. Please upgrade your license to use this feature."
                .to_owned(),
        }
    }
}

/// `licensing.GetFreeLicenseInfo`: the implicit license of an instance without
/// a configured license key. Its plan (`free-1`) allows one private
/// repository.
fn free_license_info() -> Info {
    Info {
        tags: vec!["plan:free-1".to_owned()],
        user_count: 10,
        ..Info::default()
    }
}

/// `licensing.GetConfiguredProductLicenseInfo`: parse and verify the key from
/// the site configuration, or fall back to the free license when it is empty.
pub fn configured_license_info(license_key: &str) -> Result<Info, String> {
    configured_license_info_with_signature(license_key).map(|(info, _)| info)
}

/// `licensing.GetConfiguredProductLicenseInfoWithSignature`: as
/// [`configured_license_info`], plus the raw signature bytes. An empty key
/// yields the free license and an empty signature (the baseline's
/// `GetFreeLicenseInfo` returns `""`).
pub fn configured_license_info_with_signature(
    license_key: &str,
) -> Result<(Info, Vec<u8>), String> {
    if license_key.is_empty() {
        return Ok((free_license_info(), Vec::new()));
    }
    let key = public_key()?;
    let (info, signature) = parse_signed_key_with_signature(license_key, &key)?;
    if let Some(tag) = info.unknown_plan_tag() {
        return Err(format!(
            "The license has an unrecognizable plan in tag {tag:?}, please contact Sourcegraph support."
        ));
    }
    Ok((info, signature))
}

/// `licensing.LicenseCheckInterval`: how often an unchanged license key is
/// re-checked against the license server.
pub const LICENSE_CHECK_INTERVAL_SECS: i64 = 12 * 60 * 60;

/// `licensing.LicenseValidityStoreKey`.
pub const LICENSE_VALIDITY_STORE_KEY: &str = "licensing:is_license_valid";
/// `licensing.LicenseInvalidReason`.
pub const LICENSE_INVALID_REASON: &str = "licensing:license_invalid_reason";
/// `licensing.MaxUsersKey` (`license_user_count:max`).
pub const MAX_USERS_KEY: &str = "license_user_count:max";
/// `licensing.MaxUsersTimeKey` (`license_user_count:max_time`).
pub const MAX_USERS_TIME_KEY: &str = "license_user_count:max_time";

/// `license.LicenseKeyBasedAccessTokenPrefix`.
pub const LICENSE_KEY_ACCESS_TOKEN_PREFIX: &str = "slk_";

/// `license.GenerateLicenseKeyBasedAccessToken`: `slk_` + hex(sha256(key)).
pub fn license_key_based_access_token(license_key: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(license_key.as_bytes());
    let mut token = String::with_capacity(LICENSE_KEY_ACCESS_TOKEN_PREFIX.len() + 64);
    token.push_str(LICENSE_KEY_ACCESS_TOKEN_PREFIX);
    for byte in digest {
        token.push_str(&format!("{byte:02x}"));
    }
    token
}

/// `licensing.Check((&FeaturePrivateRepositories{}))` resolved to the effective
/// limit. A missing, invalid or expired plan does not fail; only a license
/// that fails to parse/verify (or whose plan is unknown) resolves to
/// [`PrivateRepositories::NotActivated`], because the hooks turn every
/// `Check` error into the same "feature is not activated" error.
pub fn check_private_repositories(license_key: &str) -> PrivateRepositories {
    let info = match configured_license_info(license_key) {
        Ok(info) => info,
        Err(_) => return PrivateRepositories::NotActivated,
    };
    private_repositories_for(&info)
}

/// `licensing.Check(licensing.FeatureACLs)`: whether background permissions
/// syncing is licensed. An empty key falls back to the free plan (`free-1`),
/// which does not include the feature; an unparseable key deactivates it.
pub fn check_acls(license_key: &str) -> bool {
    let info = match configured_license_info(license_key) {
        Ok(info) => info,
        Err(_) => return false,
    };
    acls_for(&info)
}

/// `BasicFeature("acls").Check`: an explicit `acls` tag grants the feature,
/// otherwise `internal/licensing/data.go` enables it for every plan except
/// the free plans (`free-0`, `free-1`).
fn acls_for(info: &Info) -> bool {
    info.has_tag("acls") || !matches!(info.plan(), Plan::Free0 | Plan::Free1 | Plan::Unknown)
}

fn private_repositories_for(info: &Info) -> PrivateRepositories {
    // `FeaturePrivateRepositories.Check`: an explicit tag grants unrestricted
    // private repositories, otherwise the plan's configuration applies.
    if info.has_tag("private-repositories") {
        return PrivateRepositories::Unrestricted;
    }
    match info.plan() {
        // `internal/licensing/data.go` plan table: every plan except
        // `free-1` carries `&FeaturePrivateRepositories{Unrestricted: true}`.
        Plan::Free1 => PrivateRepositories::Limited(1),
        Plan::OldEnterprise
        | Plan::Team0
        | Plan::Enterprise0
        | Plan::Enterprise1
        | Plan::Free0
        | Plan::AirGappedEnterprise
        | Plan::CodyEnterprise
        | Plan::CodeIntelligencePlatform => PrivateRepositories::Unrestricted,
        // `PlanFromTags` never returns `Unknown`: unknown tags are skipped.
        Plan::Unknown => PrivateRepositories::Unrestricted,
    }
}

/// `ssh.Signature`.
#[derive(Debug, Deserialize)]
struct SshSignature {
    #[serde(rename = "Format")]
    format: String,
    #[serde(rename = "Blob", deserialize_with = "deserialize_base64")]
    blob: Vec<u8>,
}

/// `license.signedKey`.
#[derive(Debug, Deserialize)]
struct SignedKey {
    sig: SshSignature,
    #[serde(deserialize_with = "deserialize_base64")]
    info: Vec<u8>,
}

fn deserialize_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    STANDARD
        .decode(text.as_bytes())
        .map_err(serde::de::Error::custom)
}

/// `license.ParseSignedKey`: strip whitespace, base64url-decode, decode the
/// JSON envelope, decode the claims and verify the SSH-RSA signature over the
/// encoded claims.
pub fn parse_signed_key(text: &str, public_key: &RsaPublicKey) -> Result<Info, String> {
    parse_signed_key_with_signature(text, public_key).map(|(info, _)| info)
}

/// `license.ParseSignedKey`: as [`parse_signed_key`], also returning the raw
/// signature blob (the baseline's `signature string`, used as the
/// `license_user_count:max` hash field).
pub fn parse_signed_key_with_signature(
    text: &str,
    public_key: &RsaPublicKey,
) -> Result<(Info, Vec<u8>), String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let raw = URL_SAFE_NO_PAD
        .decode(compact.as_bytes())
        .map_err(|err| format!("license key is not valid base64: {err}"))?;
    let signed: SignedKey = serde_json::from_slice(&raw)
        .map_err(|err| format!("license key is not valid JSON: {err}"))?;
    let info: Info = serde_json::from_slice(&signed.info)
        .map_err(|err| format!("license claims are not valid JSON: {err}"))?;

    if signed.sig.format != "ssh-rsa" {
        return Err(format!(
            "license signature format {:?} is not supported (expected \"ssh-rsa\")",
            signed.sig.format
        ));
    }
    let signature = Signature::try_from(signed.sig.blob.as_slice())
        .map_err(|err| format!("license signature is invalid: {err}"))?;
    VerifyingKey::<Sha1>::new(public_key.clone())
        .verify(&signed.info, &signature)
        .map_err(|err| format!("license signature verification failed: {err}"))?;
    Ok((info, signed.sig.blob))
}

/// The builtin verification key. The baseline panics if it cannot be parsed;
/// doing so lazily here keeps the panic at the first verification.
pub fn public_key() -> Result<RsaPublicKey, String> {
    parse_authorized_key(PUBLIC_KEY_DATA)
}

/// `ssh.ParseAuthorizedKey` for the `ssh-rsa` keys used by license keys.
pub fn parse_authorized_key(text: &str) -> Result<RsaPublicKey, String> {
    let mut parts = text.split_whitespace();
    let key_type = parts
        .next()
        .ok_or_else(|| "authorized key is empty".to_owned())?;
    if key_type != "ssh-rsa" {
        return Err(format!(
            "authorized key type {key_type:?} is not supported (expected \"ssh-rsa\")"
        ));
    }
    let encoded = parts
        .next()
        .ok_or_else(|| "authorized key has no key material".to_owned())?;
    let blob = STANDARD
        .decode(encoded.as_bytes())
        .map_err(|err| format!("authorized key is not valid base64: {err}"))?;
    parse_ssh_rsa_blob(&blob)
}

/// The SSH wire encoding of an RSA public key:
/// `string "ssh-rsa"; mpint e; mpint n`.
fn parse_ssh_rsa_blob(blob: &[u8]) -> Result<RsaPublicKey, String> {
    let mut reader = WireReader { rest: blob };
    let kind = reader.string()?;
    if kind != b"ssh-rsa" {
        return Err("SSH key material is not an ssh-rsa key".to_owned());
    }
    let exponent = reader.mpint()?;
    let modulus = reader.mpint()?;
    RsaPublicKey::new(
        rsa::BigUint::from_bytes_be(&modulus),
        rsa::BigUint::from_bytes_be(&exponent),
    )
    .map_err(|err| format!("SSH RSA public key is invalid: {err}"))
}

struct WireReader<'a> {
    rest: &'a [u8],
}

impl<'a> WireReader<'a> {
    fn string(&mut self) -> Result<&'a [u8], String> {
        if self.rest.len() < 4 {
            return Err("SSH key material is truncated".to_owned());
        }
        let len =
            u32::from_be_bytes([self.rest[0], self.rest[1], self.rest[2], self.rest[3]]) as usize;
        self.rest = &self.rest[4..];
        if self.rest.len() < len {
            return Err("SSH key material is truncated".to_owned());
        }
        let (value, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(value)
    }

    fn mpint(&mut self) -> Result<Vec<u8>, String> {
        let bytes = self.string()?;
        // A leading zero byte only carries the sign bit of an otherwise
        // positive number.
        match bytes.split_first() {
            Some((0, rest)) => Ok(rest.to_vec()),
            _ => Ok(bytes.to_vec()),
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::STANDARD;
    use rand::rngs::OsRng;
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::traits::PublicKeyParts;
    use rsa::{RsaPrivateKey, RsaPublicKey};

    use super::*;

    fn encode_signed_key(signing_key: &SigningKey<Sha1>, info_json: &[u8]) -> String {
        let signature = signing_key.sign(info_json);
        let envelope = serde_json::json!({
            "sig": {
                "Format": "ssh-rsa",
                "Blob": STANDARD.encode(signature.to_vec()),
            },
            "info": STANDARD.encode(info_json),
        });
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&envelope).unwrap())
    }

    fn key_pair() -> (SigningKey<Sha1>, RsaPublicKey) {
        let private = RsaPrivateKey::new(&mut OsRng, 1024).expect("generate key");
        let public = RsaPublicKey::from(&private);
        (SigningKey::<Sha1>::new(private), public)
    }

    #[test]
    fn parses_and_verifies_a_signed_key() {
        let (signing, public) = key_pair();
        let info_json = br#"{"t":["plan:free-1"],"u":10,"e":"2030-01-01T00:00:00Z"}"#;
        let key = encode_signed_key(&signing, info_json);

        // The baseline ignores whitespace (license keys are often wrapped in
        // email), so insert some.
        let wrapped = format!("{}\n {}", &key[..10], &key[10..]);
        let info = parse_signed_key(&wrapped, &public).expect("parse");
        assert_eq!(info.tags, vec!["plan:free-1"]);
        assert_eq!(info.user_count, 10);
        assert_eq!(info.plan(), Plan::Free1);
        assert_eq!(
            private_repositories_for(&info),
            PrivateRepositories::Limited(1)
        );
    }

    #[test]
    fn rejects_a_tampered_claim() {
        let (signing, public) = key_pair();
        let key = encode_signed_key(&signing, br#"{"t":["plan:free-1"]}"#);
        let raw = URL_SAFE_NO_PAD.decode(key).unwrap();
        let mut envelope: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        // Re-encode different claims without re-signing them.
        envelope["info"] =
            serde_json::Value::String(STANDARD.encode(br#"{"t":["plan:enterprise-1"]}"#));
        let tampered = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&envelope).unwrap());
        assert!(parse_signed_key(&tampered, &public).is_err());
    }

    #[test]
    fn rejects_a_signature_from_another_key() {
        let (signing, _) = key_pair();
        let (_, other_public) = key_pair();
        let key = encode_signed_key(&signing, br#"{"t":["plan:free-1"]}"#);
        assert!(parse_signed_key(&key, &other_public).is_err());
    }

    #[test]
    fn parses_the_builtin_public_key() {
        let key = parse_authorized_key(PUBLIC_KEY_DATA).expect("builtin key parses");
        assert_eq!(key.size(), 256, "2048-bit RSA key");
    }

    #[test]
    fn no_license_key_yields_the_free_plan_limit() {
        assert_eq!(
            check_private_repositories(""),
            PrivateRepositories::Limited(1)
        );
        assert_eq!(
            PrivateRepositories::Limited(1).error_message(),
            "maximum number of private repositories included in license (1) reached"
        );
    }

    #[test]
    fn acls_follow_the_plan_table() {
        // No configured key: the implicit free-1 plan has no ACLs.
        assert!(!check_acls(""));
        // Unparseable keys deactivate the feature (`Check` returns an error).
        assert!(!check_acls("not a key"));

        let plan = |tag: &str| Info {
            tags: vec![tag.to_owned()],
            ..Info::default()
        };
        assert!(!acls_for(&plan("plan:free-0")));
        assert!(!acls_for(&plan("plan:free-1")));
        for tag in [
            "plan:old-enterprise-0",
            "plan:team-0",
            "plan:enterprise-0",
            "plan:enterprise-1",
            "plan:enterprise-air-gap-0",
            "plan:cody-enterprise-0",
            "plan:code-ai-enterprise-0",
        ] {
            assert!(acls_for(&plan(tag)), "{tag}");
        }
        // An explicit `acls` tag overrides the free plan, like
        // `BasicFeature.Check`'s tag check.
        assert!(acls_for(&Info {
            tags: vec!["plan:free-1".to_owned(), "acls".to_owned()],
            ..Info::default()
        }));
        // No plan tag at all: `PlanFromTags` falls back to the old enterprise
        // plan, which includes ACLs.
        assert!(acls_for(&Info::default()));
    }

    #[test]
    fn invalid_license_keys_deactivate_the_feature() {
        // Not base64.
        assert_eq!(
            check_private_repositories("not a key"),
            PrivateRepositories::NotActivated
        );
        // Valid base64url, invalid JSON envelope.
        let envelope = URL_SAFE_NO_PAD.encode(b"{}");
        assert_eq!(
            check_private_repositories(&envelope),
            PrivateRepositories::NotActivated
        );
        // Correctly signed by an unknown key: verification fails.
        let (signing, _) = key_pair();
        let key = encode_signed_key(&signing, br#"{"t":["plan:free-1"]}"#);
        assert_eq!(
            check_private_repositories(&key),
            PrivateRepositories::NotActivated
        );
    }

    #[test]
    fn plan_table_matches_the_baseline() {
        let plan = |tag: &str| Info {
            tags: vec![tag.to_owned()],
            ..Info::default()
        };
        for tag in [
            "plan:old-enterprise-0",
            "plan:team-0",
            "plan:enterprise-0",
            "plan:enterprise-1",
            "plan:free-0",
            "plan:enterprise-air-gap-0",
            "plan:cody-enterprise-0",
            "plan:code-ai-enterprise-0",
        ] {
            assert_eq!(
                private_repositories_for(&plan(tag)),
                PrivateRepositories::Unrestricted,
                "{tag}"
            );
        }
        assert_eq!(
            private_repositories_for(&plan("plan:free-1")),
            PrivateRepositories::Limited(1)
        );
        // No plan tag: backcompat old enterprise.
        assert_eq!(
            private_repositories_for(&Info::default()),
            PrivateRepositories::Unrestricted
        );
        // The explicit tag overrides the plan.
        assert_eq!(
            private_repositories_for(&Info {
                tags: vec!["plan:free-1".into(), " private-repositories ".into()],
                ..Info::default()
            }),
            PrivateRepositories::Unrestricted
        );
    }

    #[test]
    fn recognizes_unknown_plans() {
        let info = Info {
            tags: vec!["plan:future-9".to_owned()],
            ..Info::default()
        };
        assert_eq!(info.unknown_plan_tag(), Some("plan:future-9"));
        assert_eq!(info.plan(), Plan::OldEnterprise);
        assert_eq!(
            private_repositories_for(&Info {
                tags: vec!["plan:future-9".into()],
                ..Info::default()
            }),
            PrivateRepositories::Unrestricted,
            "PlanFromTags skips unknown plans"
        );
    }
}
