#![forbid(unsafe_code)]

//! Port of `internal/encryption` for the at-rest encryption gitserver needs:
//! decrypting external-service configurations before building clone URLs.
//!
//! Wire formats are byte-compatible with the Go baseline:
//! - `wrapper::StorableEncryptedKey` is standard base64 over the Go struct's
//!   JSON (`Mechanism`, `KeyName`, `WrappedKey`, `Ciphertext`, `Nonce`), with
//!   `[]byte` fields encoded as base64 strings.
//! - The `encrypt` mechanism is `nonce || AES-256-GCM(secret, plaintext)`.
//! - The `envelope` mechanism wraps a random 32-byte key with the key secret
//!   and uses that random key for the payload (see `envelope`).

mod aeshelper;
mod envelope;
pub mod mounted;
mod wrapper;

use std::fmt;

use async_trait::async_trait;
use serde::Serialize;

pub use mounted::{MountedKey, MountedKeyConfig};
pub use wrapper::StorableEncryptedKey;

/// `encryption.UnmigratedEncryptionKeyID`: values encrypted before the
/// keyring existed are stored in plaintext under this key ID.
pub const UNMIGRATED_ENCRYPTION_KEY_ID: &str = "unmigrated";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
}

impl Error {
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

/// `encryption.Secret`: keeps plaintext out of logs and Debug output.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// `Secret.Secret`: the unobfuscated value.
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("********")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("********")
    }
}

impl Serialize for Secret {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("********")
    }
}

/// `encryption.KeyVersion`; `json()` is stored in `encryption_key_id` and is
/// compared verbatim when reporting key mismatches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyVersion {
    #[serde(rename = "Type")]
    pub key_type: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Version")]
    pub version: String,
}

impl KeyVersion {
    /// `KeyVersion.JSON`.
    pub fn json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// `encryption.Key`: combines `Encrypter` and `Decrypter`. Operations are
/// async because KMS-backed keys perform network calls, like the Go ctx-aware
/// interface.
#[async_trait]
pub trait Key: Send + Sync {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error>;

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Secret, Error>;

    async fn version(&self) -> Result<KeyVersion, Error>;
}

/// `encryption.NoopKey`: leaves values in plaintext.
#[derive(Debug, Clone, Default)]
pub struct NoopKey {
    pub fail_decrypt: bool,
}

#[async_trait]
impl Key for NoopKey {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(plaintext.to_vec())
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Secret, Error> {
        if self.fail_decrypt {
            return Err(Error::msg("unsupported decrypt"));
        }
        Ok(Secret::new(
            String::from_utf8_lossy(ciphertext).into_owned(),
        ))
    }

    async fn version(&self) -> Result<KeyVersion, Error> {
        Ok(KeyVersion {
            key_type: "noop".to_owned(),
            name: "noop".to_owned(),
            version: String::new(),
        })
    }
}

/// `encryption.MaybeEncrypt`: a `None` key encrypts nothing. While
/// `ALLOW_DECRYPTION=true` (the database decryption worker) new values are
/// written in plaintext, too.
pub async fn maybe_encrypt(key: Option<&dyn Key>, data: &str) -> Result<(String, String), Error> {
    let Some(key) = key else {
        return Ok((data.to_owned(), String::new()));
    };
    if std::env::var("ALLOW_DECRYPTION").as_deref() == Ok("true") {
        return Ok((data.to_owned(), String::new()));
    }

    let encrypted = key.encrypt(data.as_bytes()).await?;
    let version = key
        .version()
        .await
        .map_err(|err| Error::msg(format!("failed to get encryption key version: {err}")))?;
    Ok((
        String::from_utf8_lossy(&encrypted).into_owned(),
        version.json(),
    ))
}

/// `encryption.MaybeDecrypt`: plaintext values (`""` or `unmigrated` key ID)
/// pass through unchanged. A missing key for an encrypted value, or a key that
/// does not match the stored key version, is reported with the baseline texts.
pub async fn maybe_decrypt(
    key: Option<&dyn Key>,
    data: &str,
    key_ident: &str,
) -> Result<String, Error> {
    if key_ident.is_empty() || key_ident == UNMIGRATED_ENCRYPTION_KEY_ID {
        return Ok(data.to_owned());
    }
    if data.is_empty() {
        return Ok(data.to_owned());
    }
    let Some(key) = key else {
        return Err(Error::msg(
            "key mismatch: value is encrypted but no encryption key available in site-config",
        ));
    };

    match key.decrypt(data.as_bytes()).await {
        Ok(secret) => Ok(secret.secret().to_owned()),
        Err(err) => {
            if let Ok(version) = key.version().await {
                if key_ident != version.json() {
                    return Err(Error::msg(
                        "key mismatch: value is encrypted with an encryption key distinct from the one available in site-config",
                    ));
                }
            }
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_key_round_trips_plaintext() {
        let key = NoopKey::default();
        let cipher = key.encrypt(b"hello").await.expect("encrypt");
        assert_eq!(cipher, b"hello");
        assert_eq!(
            key.decrypt(&cipher).await.expect("decrypt").secret(),
            "hello"
        );
        assert_eq!(
            key.version().await.expect("version").json(),
            r#"{"Type":"noop","Name":"noop","Version":""}"#
        );
    }

    #[tokio::test]
    async fn maybe_decrypt_passes_plaintext_and_unmigrated_values_through() {
        let key = NoopKey::default();
        assert_eq!(
            maybe_decrypt(Some(&key), "plain", "").await.expect("text"),
            "plain"
        );
        assert_eq!(
            maybe_decrypt(Some(&key), "plain", UNMIGRATED_ENCRYPTION_KEY_ID)
                .await
                .expect("unmigrated"),
            "plain"
        );
        assert_eq!(
            maybe_decrypt(None, "", "some-key").await.expect("empty"),
            ""
        );
    }

    #[tokio::test]
    async fn maybe_decrypt_reports_missing_and_mismatched_keys() {
        let err = maybe_decrypt(None, "cipher", "some-key")
            .await
            .expect_err("missing key");
        assert_eq!(
            err.to_string(),
            "key mismatch: value is encrypted but no encryption key available in site-config"
        );

        let key = NoopKey { fail_decrypt: true };
        let err = maybe_decrypt(Some(&key), "cipher", "some-key")
            .await
            .expect_err("mismatch");
        assert_eq!(
            err.to_string(),
            "key mismatch: value is encrypted with an encryption key distinct from the one available in site-config"
        );
    }

    #[test]
    fn secret_never_renders_its_value() {
        let secret = Secret::new("hunter2");
        assert_eq!(format!("{secret}"), "********");
        assert_eq!(format!("{secret:?}"), "********");
        assert_eq!(
            serde_json::to_string(&secret).expect("json"),
            "\"********\""
        );
        assert_eq!(secret.secret(), "hunter2");
    }
}
