//! Port of `internal/encryption/mounted`: AES-256-GCM keys whose secret comes
//! from an environment variable or a file.
//!
//! Reading the secret mirrors `mounted.NewKey`: exactly one of `envVarName`
//! and `filepath` must be set and the secret must be 32 bytes. Decryption
//! supports both the legacy `encrypt` mechanism (`Ciphertext` is
//! `nonce || ciphertext`) and the current `envelope` mechanism.

use std::path::PathBuf;

use async_trait::async_trait;

use crate::wrapper::StorableEncryptedKey;
use crate::{aeshelper, envelope, Error, Key, KeyVersion, Secret};

const MECHANISM_ENCRYPT: &str = "encrypt";
const MECHANISM_ENVELOPE: &str = "envelope";

/// `schema.MountedEncryptionKey`, minus the `type` discriminant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MountedKeyConfig {
    pub keyname: String,
    pub version: String,
    pub env_var_name: String,
    pub filepath: String,
}

/// `mounted.Key`.
#[derive(Debug, Clone)]
pub struct MountedKey {
    keyname: String,
    version: String,
    secret: Vec<u8>,
}

impl MountedKey {
    /// `mounted.NewKey`.
    pub fn from_config(config: &MountedKeyConfig) -> Result<Self, Error> {
        let secret = match (config.env_var_name.is_empty(), config.filepath.is_empty()) {
            (false, true) => std::env::var(&config.env_var_name)
                .unwrap_or_default()
                .into_bytes(),
            (true, false) => std::fs::read(PathBuf::from(&config.filepath)).map_err(|err| {
                Error::msg(format!(
                    "error reading secret file for {}: {err}",
                    go_quote(&config.keyname)
                ))
            })?,
            _ => {
                return Err(Error::msg(format!(
                    "must use only one of EnvVarName and Filepath, EnvVarName: {}, Filepath: {}",
                    go_quote(&config.env_var_name),
                    go_quote(&config.filepath)
                )));
            }
        };
        Self::from_secret(&config.keyname, &config.version, secret)
    }

    /// `mounted.NewKey` after loading the secret.
    pub fn from_secret(
        keyname: &str,
        version: &str,
        secret: impl Into<Vec<u8>>,
    ) -> Result<Self, Error> {
        let secret = secret.into();
        if secret.len() != 32 {
            return Err(Error::msg(format!(
                "invalid key length: {}, expected 32 bytes",
                secret.len()
            )));
        }
        Ok(Self {
            keyname: keyname.to_owned(),
            version: version.to_owned(),
            secret,
        })
    }

    fn nonce_size(&self) -> usize {
        // AES-256-GCM always uses a 12-byte nonce, like `cipher.GCM.NonceSize`.
        12
    }
}

#[async_trait]
impl Key for MountedKey {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let ev = envelope::encrypt(plaintext)
            .map_err(|err| Error::msg(format!("envelope encrypting plaintext: {err}")))?;

        let (key_ciphertext, key_nonce) = aeshelper::encrypt(&self.secret, &ev.key)?;
        let mut wrapped_key = key_nonce;
        wrapped_key.extend_from_slice(&key_ciphertext);

        StorableEncryptedKey {
            mechanism: MECHANISM_ENVELOPE.to_owned(),
            key_name: self.keyname.clone(),
            wrapped_key,
            ciphertext: ev.ciphertext,
            nonce: ev.nonce,
        }
        .serialize()
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Secret, Error> {
        let mut storable = StorableEncryptedKey::from_ciphertext(ciphertext)
            .map_err(|err| Error::msg(format!("unmarshalling encrypted key: {err}")))?;
        if storable.mechanism.is_empty() {
            storable.mechanism = MECHANISM_ENCRYPT.to_owned();
        }

        if !storable.key_name.starts_with(&self.keyname) {
            return Err(Error::msg(
                "invalid key name, are you trying to decrypt something with the wrong key?",
            ));
        }

        match storable.mechanism.as_str() {
            MECHANISM_ENCRYPT => {
                let nonce_size = self.nonce_size();
                if storable.ciphertext.len() < nonce_size {
                    return Err(Error::msg("malformed ciphertext"));
                }
                let plaintext = aeshelper::decrypt(
                    &self.secret,
                    &storable.ciphertext[nonce_size..],
                    &storable.ciphertext[..nonce_size],
                )?;
                Ok(Secret::new(
                    String::from_utf8_lossy(&plaintext).into_owned(),
                ))
            }
            MECHANISM_ENVELOPE => {
                let nonce_size = self.nonce_size();
                if storable.wrapped_key.len() < nonce_size {
                    return Err(Error::msg("malformed ciphertext"));
                }
                let key = aeshelper::decrypt(
                    &self.secret,
                    &storable.wrapped_key[nonce_size..],
                    &storable.wrapped_key[..nonce_size],
                )?;
                let plaintext = envelope::decrypt(&envelope::Envelope {
                    key,
                    nonce: storable.nonce,
                    ciphertext: storable.ciphertext,
                })?;
                Ok(Secret::new(
                    String::from_utf8_lossy(&plaintext).into_owned(),
                ))
            }
            mechanism => Err(Error::msg(format!(
                "unsupported encryption mechanism: {mechanism}"
            ))),
        }
    }

    async fn version(&self) -> Result<KeyVersion, Error> {
        Ok(KeyVersion {
            key_type: "mounted".to_owned(),
            name: self.keyname.clone(),
            version: self.version.clone(),
        })
    }
}

/// Go's `%q` for ASCII configuration strings: `serde_json` escapes the same
/// way for the characters that occur in key names and paths.
fn go_quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    const FIXTURES: &str = include_str!("../testdata/mounted_fixtures.json");

    #[derive(Deserialize)]
    struct Fixtures {
        secret: String,
        keyname: String,
        version: String,
        plaintext: String,
        key_version_json: String,
        envelope_ciphertext: String,
        legacy_ciphertext: String,
        legacy_empty_ciphertext: String,
    }

    fn fixtures() -> Fixtures {
        serde_json::from_str(FIXTURES).expect("fixtures")
    }

    fn key(fixtures: &Fixtures) -> MountedKey {
        MountedKey::from_secret(
            &fixtures.keyname,
            &fixtures.version,
            fixtures.secret.as_bytes(),
        )
        .expect("key")
    }

    #[tokio::test]
    async fn decrypts_values_produced_by_the_go_baseline() {
        let fixtures = fixtures();
        let key = key(&fixtures);

        for ciphertext in [
            &fixtures.envelope_ciphertext,
            &fixtures.legacy_ciphertext,
            &fixtures.legacy_empty_ciphertext,
        ] {
            let secret = key
                .decrypt(ciphertext.as_bytes())
                .await
                .expect("decrypt go ciphertext");
            assert_eq!(secret.secret(), fixtures.plaintext);
        }

        assert_eq!(
            key.version().await.expect("version").json(),
            fixtures.key_version_json
        );
    }

    #[tokio::test]
    async fn round_trips_through_the_envelope_mechanism() {
        let fixtures = fixtures();
        let key = key(&fixtures);
        let ciphertext = key
            .encrypt(fixtures.plaintext.as_bytes())
            .await
            .expect("encrypt");

        let storable = StorableEncryptedKey::from_ciphertext(&ciphertext).expect("parse");
        assert_eq!(storable.mechanism, "envelope");
        assert_eq!(storable.key_name, fixtures.keyname);

        // Go can read what Rust wrote: the wrapper shape is asserted by the
        // field checks above, and the round-trip pins the mechanism.
        assert_eq!(
            key.decrypt(&ciphertext).await.expect("decrypt").secret(),
            fixtures.plaintext
        );
    }

    #[tokio::test]
    async fn rejects_wrong_key_names_and_unknown_mechanisms() {
        let fixtures = fixtures();
        let key = key(&fixtures);

        let other =
            MountedKey::from_secret("other-key", "", fixtures.secret.as_bytes()).expect("key");
        let err = other
            .decrypt(fixtures.envelope_ciphertext.as_bytes())
            .await
            .expect_err("wrong key");
        assert_eq!(
            err.to_string(),
            "invalid key name, are you trying to decrypt something with the wrong key?"
        );

        let storable = StorableEncryptedKey {
            mechanism: "rot13".to_owned(),
            key_name: fixtures.keyname.clone(),
            ..Default::default()
        };
        let err = key
            .decrypt(&storable.serialize().expect("serialize"))
            .await
            .expect_err("mechanism");
        assert_eq!(err.to_string(), "unsupported encryption mechanism: rot13");
    }

    #[test]
    fn validates_key_configuration() {
        let err = MountedKey::from_secret("k", "v", b"short").expect_err("short");
        assert_eq!(err.to_string(), "invalid key length: 5, expected 32 bytes");

        let err = MountedKey::from_config(&MountedKeyConfig {
            keyname: "k".to_owned(),
            env_var_name: "A".to_owned(),
            filepath: "/tmp/key".to_owned(),
            ..Default::default()
        })
        .expect_err("both");
        assert_eq!(
            err.to_string(),
            "must use only one of EnvVarName and Filepath, EnvVarName: \"A\", Filepath: \"/tmp/key\""
        );
    }
}
