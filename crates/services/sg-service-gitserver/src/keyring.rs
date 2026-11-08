//! The subset of `internal/encryption/keyring` gitserver needs: the
//! `externalServiceKey` used to decrypt `external_services.config`.
//!
//! `shared.Main` calls `keyring.Init`, which builds every configured key and
//! fails startup when one cannot be created. The Rust gitserver only reads
//! `externalServiceKey`, so other entries are not constructed; `awskms` and
//! `cloudkms` keys are not supported yet and report an error instead of
//! silently serving undecryptable configs.

use std::sync::Arc;

use sg_config::{EncryptionKeyConfig, SiteConfiguration};
use sg_encryption::{Key, MountedKey, MountedKeyConfig, NoopKey};

/// Build the `externalServiceKey` from `encryption.keys.externalServiceKey`.
/// `Ok(None)` when the site config does not configure the keyring.
pub fn external_service_key(site: &SiteConfiguration) -> Result<Option<Arc<dyn Key>>, String> {
    let Some(keys) = site.encryption_keys() else {
        return Ok(None);
    };
    let Some(key) = keys.external_service_key else {
        return Ok(None);
    };
    Ok(Some(new_key(&key)?))
}

fn new_key(config: &EncryptionKeyConfig) -> Result<Arc<dyn Key>, String> {
    match config {
        EncryptionKeyConfig::Mounted(mounted) => {
            let key = MountedKey::from_config(&MountedKeyConfig {
                keyname: mounted.keyname.clone(),
                version: mounted.version.clone(),
                env_var_name: mounted.env_var_name.clone(),
                filepath: mounted.filepath.clone(),
            })
            .map_err(|err| err.to_string())?;
            Ok(Arc::new(key))
        }
        EncryptionKeyConfig::Noop => Ok(Arc::new(NoopKey::default())),
        EncryptionKeyConfig::Awskms(_) => {
            Err("awskms encryption keys are not supported by the Rust gitserver yet".to_owned())
        }
        EncryptionKeyConfig::Cloudkms(_) => {
            Err("cloudkms encryption keys are not supported by the Rust gitserver yet".to_owned())
        }
        EncryptionKeyConfig::Unknown(value) => Err(format!(
            "couldn't configure key: {}",
            serde_json::to_string(value).unwrap_or_default()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_encryption_configuration_means_no_key() {
        assert!(external_service_key(&SiteConfiguration::default())
            .expect("none")
            .is_none());
    }

    #[test]
    fn builds_mounted_and_noop_keys_and_rejects_the_rest() {
        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {"type": "noop"}}}"#,
        )
        .expect("parse");
        assert!(external_service_key(&site).expect("noop").is_some());

        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {"type": "awskms", "key": "arn"}}}"#,
        )
        .expect("parse");
        let err = external_service_key(&site).err().expect("awskms");
        assert!(err.contains("awskms encryption keys are not supported"));

        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {"type": "mystery"}}}"#,
        )
        .expect("parse");
        let err = external_service_key(&site).err().expect("unknown");
        assert!(err.starts_with("couldn't configure key: "));
    }

    #[test]
    fn mounted_key_requires_a_readable_secret() {
        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {
                "type": "mounted", "keyname": "k", "filepath": "/nonexistent/secret",
            }}}"#,
        )
        .expect("parse");
        let err = external_service_key(&site).err().expect("missing file");
        assert!(err.contains("error reading secret file for \"k\""), "{err}");
    }
}
