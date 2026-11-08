//! Port of `internal/encryption/awskms` and `internal/encryption/cloudkms`.
//!
//! Both keys encrypt payloads with the shared envelope format and fall back to
//! the legacy `encrypt` mechanism. They differ in how the per-value envelope
//! key is wrapped and in the key-name/checksum validation, which is modelled
//! with [`KmsTransport`]: the SDK-backed transports live in [`crate::awskms`]
//! and [`crate::cloudkms`], while tests drive the exact byte formats without a
//! cloud account.
//!
//! Wire compatibility with the Go baseline:
//! - `envelope` values are `wrapper.StorableEncryptedKey` with `WrappedKey`
//!   holding the KMS ciphertext (no AES layer) and `KeyName` holding the key
//!   identifier returned by the KMS service.
//! - Legacy AWS values are `base64(json({Key, Nonce, Ciphertext}))` where
//!   `Key` is the KMS-wrapped data key and `Nonce || Ciphertext` is the
//!   AES-256-GCM payload.
//! - Legacy Cloud KMS values are the same `StorableEncryptedKey` JSON with an
//!   empty `Mechanism` and `Ciphertext` already encrypted by KMS.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use crc::{Crc, CRC_32_ISCSI};
use serde::Deserialize;

use crate::wrapper::{deserialize_bytes, go_base64_error};
use crate::{aeshelper, envelope, Error, Key, KeyVersion, Secret, StorableEncryptedKey};

const MECHANISM_ENCRYPT: &str = "encrypt";
const MECHANISM_ENVELOPE: &str = "envelope";

const ERR_WRONG_KEY: &str =
    "invalid key name, are you trying to decrypt something with the wrong key?";
const ERR_DECRYPT_CHECKSUM: &str =
    "invalid checksum, either the wrong key was used, or the request was corrupted in transit";
const ERR_ENCRYPT_CHECKSUM: &str = "invalid checksum, request corrupted in transit";

/// `crc32.Checksum(data, crc32.Castagnoli)`: Cloud KMS verifies payloads with
/// CRC32C. `aws-kms` does not use it, but computing it unconditionally keeps
/// the transport trait shared.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    static CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
    CRC.checksum(data)
}

/// The KMS `DescribeKey`/`GetCryptoKey` result the baseline fetches when a key
/// is constructed and on every `Version` call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KmsMetadata {
    /// `KeyMetadata.KeyId` / `CryptoKey.Name`.
    pub name: String,
    /// `KeyMetadata.Arn` / primary `CryptoKeyVersion.Name`.
    pub version: String,
}

/// `kms.EncryptOutput` / `kmspb.EncryptResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KmsEncryptOutput {
    /// `EncryptOutput.KeyId` / `EncryptResponse.Name`.
    pub key_name: String,
    pub ciphertext: Vec<u8>,
    /// Cloud KMS only: `VerifiedPlaintextCrc32C`.
    pub verified_plaintext_crc32c: bool,
    /// Cloud KMS only: `CiphertextCrc32C`.
    pub ciphertext_crc32c: Option<i64>,
}

/// `kms.DecryptOutput` / `kmspb.DecryptResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KmsDecrypted {
    pub plaintext: Vec<u8>,
    /// Cloud KMS only: `PlaintextCrc32C`, validated against `plaintext`.
    pub plaintext_crc32c: Option<i64>,
}

/// The subset of a KMS API the two key types use.
#[async_trait]
pub trait KmsTransport: Send + Sync + fmt::Debug {
    /// `DescribeKey` / `GetCryptoKey`.
    async fn describe(&self) -> Result<KmsMetadata, Error>;
    /// Encrypts a 32-byte envelope key.
    async fn encrypt(&self, plaintext: &[u8]) -> Result<KmsEncryptOutput, Error>;
    /// Decrypts an envelope key (or a legacy payload) with the configured key.
    async fn decrypt(&self, ciphertext: &[u8]) -> Result<KmsDecrypted, Error>;
}

impl fmt::Debug for AwsKmsKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKmsKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// `awskms.Key`.
pub struct AwsKmsKey {
    key_id: String,
    transport: Arc<dyn KmsTransport>,
}

impl AwsKmsKey {
    /// `awskms.newKey`: tests the client connection with a `DescribeKey` call.
    pub async fn new(
        key_id: impl Into<String>,
        transport: Arc<dyn KmsTransport>,
    ) -> Result<Self, Error> {
        let key = Self {
            key_id: key_id.into(),
            transport,
        };
        key.version().await?;
        Ok(key)
    }

    /// A key that skips the constructor's `DescribeKey` probe (test hook, like
    /// `newKeyWithClient` in the Cloud KMS tests).
    pub fn with_transport(key_id: impl Into<String>, transport: Arc<dyn KmsTransport>) -> Self {
        Self {
            key_id: key_id.into(),
            transport,
        }
    }
}

#[async_trait]
impl Key for AwsKmsKey {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let ev = envelope::encrypt(plaintext)
            .map_err(|err| Error::msg(format!("envelope encrypting payload: {err}")))?;

        let res = self
            .transport
            .encrypt(&ev.key)
            .await
            .map_err(|err| Error::msg(format!("encrypting envelope key: {err}")))?;

        StorableEncryptedKey {
            mechanism: MECHANISM_ENVELOPE.to_owned(),
            key_name: res.key_name,
            wrapped_key: res.ciphertext,
            ciphertext: ev.ciphertext,
            nonce: ev.nonce,
        }
        .serialize()
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Secret, Error> {
        let mut wr = StorableEncryptedKey::from_ciphertext(ciphertext)?;
        if wr.mechanism.is_empty() {
            wr.mechanism = MECHANISM_ENCRYPT.to_owned();
        }

        match wr.mechanism.as_str() {
            MECHANISM_ENCRYPT => {
                let decoded = STANDARD
                    .decode(ciphertext)
                    .map_err(|err| Error::msg(go_base64_error(&err, ciphertext.len())))?;
                if decoded.is_empty() {
                    return Err(Error::msg("unexpected end of JSON input"));
                }
                let ev: AwsEncryptedValue = serde_json::from_slice(&decoded)
                    .map_err(|err| Error::msg(format!("unmarshalling encrypted value: {err}")))?;

                let key = self.transport.decrypt(&ev.key).await?.plaintext;
                let plaintext = aeshelper::decrypt(&key, &ev.ciphertext, &ev.nonce)?;
                Ok(Secret::new(
                    String::from_utf8_lossy(&plaintext).into_owned(),
                ))
            }
            MECHANISM_ENVELOPE => {
                if !wr.key_name.ends_with(&self.key_id) {
                    return Err(Error::msg(ERR_WRONG_KEY));
                }
                let key = self.transport.decrypt(&wr.wrapped_key).await?.plaintext;
                let plaintext = envelope::decrypt(&envelope::Envelope {
                    key,
                    nonce: wr.nonce,
                    ciphertext: wr.ciphertext,
                })?;
                Ok(Secret::new(
                    String::from_utf8_lossy(&plaintext).into_owned(),
                ))
            }
            mechanism => Err(Error::msg(format!(
                "invalid mechanism {}",
                go_quote(mechanism)
            ))),
        }
    }

    async fn version(&self) -> Result<KeyVersion, Error> {
        let metadata = self
            .transport
            .describe()
            .await
            .map_err(|err| Error::msg(format!("getting key version: {err}")))?;
        Ok(KeyVersion {
            key_type: "awskms".to_owned(),
            name: metadata.name,
            version: metadata.version,
        })
    }
}

impl fmt::Debug for CloudKmsKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudKmsKey")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// `cloudkms.Key`.
pub struct CloudKmsKey {
    name: String,
    transport: Arc<dyn KmsTransport>,
}

impl CloudKmsKey {
    /// `cloudkms.newKeyWithClient`: tests the client connection with a
    /// `GetCryptoKey` call.
    pub async fn new(
        name: impl Into<String>,
        transport: Arc<dyn KmsTransport>,
    ) -> Result<Self, Error> {
        let key = Self {
            name: name.into(),
            transport,
        };
        key.version().await?;
        Ok(key)
    }

    /// A key that skips the constructor's `GetCryptoKey` probe (test hook).
    pub fn with_transport(name: impl Into<String>, transport: Arc<dyn KmsTransport>) -> Self {
        Self {
            name: name.into(),
            transport,
        }
    }
}

#[async_trait]
impl Key for CloudKmsKey {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let ev = envelope::encrypt(plaintext)
            .map_err(|err| Error::msg(format!("envelope encrypting payload: {err}")))?;

        let res = self
            .transport
            .encrypt(&ev.key)
            .await
            .map_err(|err| Error::msg(format!("encrypting envelope key: {err}")))?;

        // `Key.Encrypt` validates both checksums returned by the service.
        if !res.verified_plaintext_crc32c
            || res.ciphertext_crc32c != Some(i64::from(crc32c(&res.ciphertext)))
        {
            return Err(Error::msg(ERR_ENCRYPT_CHECKSUM));
        }

        StorableEncryptedKey {
            mechanism: MECHANISM_ENVELOPE.to_owned(),
            key_name: res.key_name,
            wrapped_key: res.ciphertext,
            ciphertext: ev.ciphertext,
            nonce: ev.nonce,
        }
        .serialize()
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Secret, Error> {
        let mut wr = StorableEncryptedKey::from_ciphertext(ciphertext)?;
        if wr.mechanism.is_empty() {
            wr.mechanism = MECHANISM_ENCRYPT.to_owned();
        }

        // Unlike AWS, Cloud KMS validates the key name before dispatching on
        // the mechanism, so legacy values carry a `KeyName` in their wrapper.
        if !wr.key_name.starts_with(&self.name) {
            return Err(Error::msg(ERR_WRONG_KEY));
        }

        match wr.mechanism.as_str() {
            MECHANISM_ENCRYPT => {
                let res = self.transport.decrypt(&wr.ciphertext).await?;
                // `GetValue` on a missing checksum is 0 in Go.
                let checksum = res.plaintext_crc32c.unwrap_or(0);
                if i64::from(crc32c(&res.plaintext)) != checksum {
                    return Err(Error::msg(ERR_DECRYPT_CHECKSUM));
                }
                Ok(Secret::new(
                    String::from_utf8_lossy(&res.plaintext).into_owned(),
                ))
            }
            MECHANISM_ENVELOPE => {
                let key = self.transport.decrypt(&wr.wrapped_key).await?.plaintext;
                let plaintext = envelope::decrypt(&envelope::Envelope {
                    key,
                    nonce: wr.nonce,
                    ciphertext: wr.ciphertext,
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
        let metadata = self
            .transport
            .describe()
            .await
            .map_err(|err| Error::msg(format!("getting key version: {err}")))?;
        Ok(KeyVersion {
            key_type: "cloudkms".to_owned(),
            name: metadata.name,
            version: metadata.version,
        })
    }
}

/// `awskms.encryptedValue`.
#[derive(Debug, Default, Deserialize)]
struct AwsEncryptedValue {
    #[serde(rename = "Key", default, deserialize_with = "deserialize_bytes")]
    key: Vec<u8>,
    #[serde(rename = "Nonce", default, deserialize_with = "deserialize_bytes")]
    nonce: Vec<u8>,
    #[serde(rename = "Ciphertext", default, deserialize_with = "deserialize_bytes")]
    ciphertext: Vec<u8>,
}

/// Go's `%q` for the mechanism strings reported in errors.
fn go_quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory stand-in for the KMS service. `wrap`/`unwrap` model the
    /// service's ciphertext, and every call is recorded so tests can assert
    /// the envelopes that reach the service.
    #[derive(Debug, Default)]
    struct FakeTransport {
        name: String,
        version: String,
        /// Prefix added to plaintext by `encrypt`; removed by `decrypt`.
        wrap_prefix: Vec<u8>,
        calls: Mutex<Vec<String>>,
        /// When set, `decrypt` returns plaintext with this checksum instead of
        /// the correct one.
        corrupt_checksum: bool,
        /// When set, `encrypt` reports a mismatched ciphertext checksum.
        corrupt_ciphertext_crc: bool,
    }

    impl FakeTransport {
        fn new(name: &str, version: &str) -> Self {
            Self {
                name: name.to_owned(),
                version: version.to_owned(),
                wrap_prefix: b"wrapped:".to_vec(),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl KmsTransport for FakeTransport {
        async fn describe(&self) -> Result<KmsMetadata, Error> {
            self.calls
                .lock()
                .expect("calls")
                .push("describe".to_owned());
            Ok(KmsMetadata {
                name: self.name.clone(),
                version: self.version.clone(),
            })
        }

        async fn encrypt(&self, plaintext: &[u8]) -> Result<KmsEncryptOutput, Error> {
            self.calls.lock().expect("calls").push("encrypt".to_owned());
            let mut ciphertext = self.wrap_prefix.clone();
            ciphertext.extend_from_slice(plaintext);
            let mut crc = i64::from(crc32c(&ciphertext));
            if self.corrupt_ciphertext_crc {
                crc += 1;
            }
            Ok(KmsEncryptOutput {
                key_name: self.name.clone(),
                ciphertext,
                verified_plaintext_crc32c: true,
                ciphertext_crc32c: Some(crc),
            })
        }

        async fn decrypt(&self, ciphertext: &[u8]) -> Result<KmsDecrypted, Error> {
            self.calls.lock().expect("calls").push("decrypt".to_owned());
            let plaintext = ciphertext
                .strip_prefix(self.wrap_prefix.as_slice())
                .ok_or_else(|| Error::msg("unwrapping key: invalid ciphertext"))?;
            let mut crc = i64::from(crc32c(plaintext));
            if self.corrupt_checksum {
                crc += 1;
            }
            Ok(KmsDecrypted {
                plaintext: plaintext.to_vec(),
                plaintext_crc32c: Some(crc),
            })
        }
    }

    const AWS_KEY_ID: &str = "4b739277-5a93-4551-b71c-99608c9c805d";
    const CLOUD_KEY_NAME: &str =
        "projects/erik-test-kms/locations/us/keyRings/erik-test/cryptoKeys/test-kms";

    #[tokio::test]
    async fn aws_key_round_trips_through_the_envelope_mechanism() {
        let transport = Arc::new(FakeTransport::new(AWS_KEY_ID, "arn:aws:kms:key/1"));
        let key = AwsKmsKey::new(AWS_KEY_ID, transport.clone())
            .await
            .expect("new");

        let ciphertext = key.encrypt(b"hunter2").await.expect("encrypt");
        let storable = StorableEncryptedKey::from_ciphertext(&ciphertext).expect("parse");
        assert_eq!(storable.mechanism, "envelope");
        assert_eq!(storable.key_name, AWS_KEY_ID);
        // The wrapped key is the KMS ciphertext of a 32-byte envelope key.
        assert!(storable.wrapped_key.starts_with(b"wrapped:"));
        assert_eq!(storable.wrapped_key.len(), b"wrapped:".len() + 32);

        assert_eq!(
            key.decrypt(&ciphertext).await.expect("decrypt").secret(),
            "hunter2"
        );
        assert_eq!(
            key.version().await.expect("version").json(),
            r#"{"Type":"awskms","Name":"4b739277-5a93-4551-b71c-99608c9c805d","Version":"arn:aws:kms:key/1"}"#
        );
    }

    #[tokio::test]
    async fn aws_key_decrypts_the_legacy_encrypt_mechanism() {
        let key =
            AwsKmsKey::with_transport(AWS_KEY_ID, Arc::new(FakeTransport::new(AWS_KEY_ID, "arn")));

        // Simulate `awskms.oldEncrypt`: KMS-wrapped data key + AES-GCM.
        let data_key = [7u8; 32];
        let (aes_ciphertext, nonce) = aeshelper::encrypt(&data_key, b"legacy secret").expect("aes");
        let mut wrapped_key = b"wrapped:".to_vec();
        wrapped_key.extend_from_slice(&data_key);
        let legacy = serde_json::json!({
            "Key": STANDARD.encode(&wrapped_key),
            "Nonce": STANDARD.encode(&nonce),
            "Ciphertext": STANDARD.encode(&aes_ciphertext),
        });
        let ciphertext = STANDARD.encode(serde_json::to_vec(&legacy).expect("json"));

        let secret = key
            .decrypt(ciphertext.as_bytes())
            .await
            .expect("legacy decrypt");
        assert_eq!(secret.secret(), "legacy secret");
    }

    #[tokio::test]
    async fn aws_key_validates_names_and_mechanisms() {
        let key =
            AwsKmsKey::with_transport(AWS_KEY_ID, Arc::new(FakeTransport::new(AWS_KEY_ID, "arn")));

        let wrong = StorableEncryptedKey {
            mechanism: "envelope".to_owned(),
            key_name: "someone-elses-key".to_owned(),
            wrapped_key: vec![1],
            ciphertext: vec![2],
            nonce: vec![3],
        }
        .serialize()
        .expect("serialize");
        assert_eq!(
            key.decrypt(&wrong)
                .await
                .expect_err("wrong key")
                .to_string(),
            ERR_WRONG_KEY
        );

        let unknown = StorableEncryptedKey {
            mechanism: "mystery".to_owned(),
            key_name: AWS_KEY_ID.to_owned(),
            ..StorableEncryptedKey::default()
        }
        .serialize()
        .expect("serialize");
        assert_eq!(
            key.decrypt(&unknown)
                .await
                .expect_err("mechanism")
                .to_string(),
            "invalid mechanism \"mystery\""
        );
    }

    #[tokio::test]
    async fn cloud_key_round_trips_and_validates_checksums() {
        let transport = Arc::new(FakeTransport::new(CLOUD_KEY_NAME, "version/1"));
        let key = CloudKmsKey::new(CLOUD_KEY_NAME, transport.clone())
            .await
            .expect("new");

        let ciphertext = key.encrypt(b"payload").await.expect("encrypt");
        let storable = StorableEncryptedKey::from_ciphertext(&ciphertext).expect("parse");
        assert_eq!(storable.mechanism, "envelope");
        assert_eq!(storable.key_name, CLOUD_KEY_NAME);
        assert_eq!(
            key.decrypt(&ciphertext).await.expect("decrypt").secret(),
            "payload"
        );
        assert_eq!(
            key.version().await.expect("version").json(),
            r#"{"Type":"cloudkms","Name":"projects/erik-test-kms/locations/us/keyRings/erik-test/cryptoKeys/test-kms","Version":"version/1"}"#
        );

        let corrupt = CloudKmsKey::with_transport(
            CLOUD_KEY_NAME,
            Arc::new(FakeTransport {
                corrupt_ciphertext_crc: true,
                ..FakeTransport::new(CLOUD_KEY_NAME, "version/1")
            }),
        );
        assert_eq!(
            corrupt
                .encrypt(b"payload")
                .await
                .expect_err("crc")
                .to_string(),
            ERR_ENCRYPT_CHECKSUM
        );
    }

    #[tokio::test]
    async fn cloud_key_decrypts_the_legacy_encrypt_mechanism_with_a_checksum() {
        let good = CloudKmsKey::with_transport(
            CLOUD_KEY_NAME,
            Arc::new(FakeTransport::new(CLOUD_KEY_NAME, "version/1")),
        );
        let mut wrapped = b"wrapped:".to_vec();
        wrapped.extend_from_slice(b"verysecrettestvalue");
        let legacy = StorableEncryptedKey {
            mechanism: String::new(),
            key_name: CLOUD_KEY_NAME.to_owned(),
            ciphertext: wrapped,
            ..StorableEncryptedKey::default()
        }
        .serialize()
        .expect("serialize");

        assert_eq!(
            good.decrypt(&legacy).await.expect("legacy").secret(),
            "verysecrettestvalue"
        );

        let corrupted = CloudKmsKey::with_transport(
            CLOUD_KEY_NAME,
            Arc::new(FakeTransport {
                corrupt_checksum: true,
                ..FakeTransport::new(CLOUD_KEY_NAME, "version/1")
            }),
        );
        assert_eq!(
            corrupted
                .decrypt(&legacy)
                .await
                .expect_err("checksum")
                .to_string(),
            ERR_DECRYPT_CHECKSUM
        );
    }

    #[tokio::test]
    async fn cloud_key_validates_the_key_name_prefix() {
        let key = CloudKmsKey::with_transport(
            CLOUD_KEY_NAME,
            Arc::new(FakeTransport::new(CLOUD_KEY_NAME, "version/1")),
        );
        let wrong = StorableEncryptedKey {
            mechanism: "encrypt".to_owned(),
            key_name: "projects/other".to_owned(),
            ..StorableEncryptedKey::default()
        }
        .serialize()
        .expect("serialize");
        assert_eq!(
            key.decrypt(&wrong).await.expect_err("prefix").to_string(),
            ERR_WRONG_KEY
        );

        let unknown = StorableEncryptedKey {
            mechanism: "mystery".to_owned(),
            key_name: CLOUD_KEY_NAME.to_owned(),
            ..StorableEncryptedKey::default()
        }
        .serialize()
        .expect("serialize");
        assert_eq!(
            key.decrypt(&unknown)
                .await
                .expect_err("mechanism")
                .to_string(),
            "unsupported encryption mechanism: mystery"
        );
    }

    #[tokio::test]
    async fn constructors_probe_the_service() {
        let transport = Arc::new(FakeTransport::new(AWS_KEY_ID, "arn"));
        let _ = AwsKmsKey::new(AWS_KEY_ID, transport.clone())
            .await
            .expect("new");
        assert_eq!(*transport.calls.lock().expect("calls"), vec!["describe"]);

        #[derive(Debug)]
        struct FailingTransport;
        #[async_trait]
        impl KmsTransport for FailingTransport {
            async fn describe(&self) -> Result<KmsMetadata, Error> {
                Err(Error::msg("connection refused"))
            }
            async fn encrypt(&self, _: &[u8]) -> Result<KmsEncryptOutput, Error> {
                unreachable!()
            }
            async fn decrypt(&self, _: &[u8]) -> Result<KmsDecrypted, Error> {
                unreachable!()
            }
        }

        let err = AwsKmsKey::new(AWS_KEY_ID, Arc::new(FailingTransport))
            .await
            .expect_err("describe fails");
        assert_eq!(err.to_string(), "getting key version: connection refused");
        let err = CloudKmsKey::new(CLOUD_KEY_NAME, Arc::new(FailingTransport))
            .await
            .expect_err("describe fails");
        assert_eq!(err.to_string(), "getting key version: connection refused");
    }

    #[test]
    fn crc32c_matches_the_castagnoli_table() {
        // Known value: crc32c("123456789") == 0xE3069283.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    /// `testdata/kms_fixtures.json` was produced by a Go program using the
    /// baseline `aeshelper`/`wrapper` packages (`awskms.oldEncrypt` and the
    /// `cloudkms` legacy wrapper shapes).
    #[tokio::test]
    async fn decrypts_legacy_values_produced_by_the_go_baseline() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct KmsFixtures {
            aws_key_id: String,
            aws_legacy_ciphertext: String,
            aws_legacy_empty_ciphertext: String,
            cloud_key_name: String,
            cloud_legacy_ciphertext: String,
            plaintext: String,
        }

        let fixtures: KmsFixtures =
            serde_json::from_str(include_str!("../testdata/kms_fixtures.json")).expect("fixtures");

        let aws = AwsKmsKey::with_transport(
            &fixtures.aws_key_id,
            Arc::new(FakeTransport::new(&fixtures.aws_key_id, "arn")),
        );
        assert_eq!(
            aws.decrypt(fixtures.aws_legacy_ciphertext.as_bytes())
                .await
                .expect("aws legacy")
                .secret(),
            fixtures.plaintext
        );
        assert_eq!(
            aws.decrypt(fixtures.aws_legacy_empty_ciphertext.as_bytes())
                .await
                .expect("aws legacy empty")
                .secret(),
            ""
        );

        let cloud = CloudKmsKey::with_transport(
            &fixtures.cloud_key_name,
            Arc::new(FakeTransport::new(&fixtures.cloud_key_name, "version/1")),
        );
        assert_eq!(
            cloud
                .decrypt(fixtures.cloud_legacy_ciphertext.as_bytes())
                .await
                .expect("cloud legacy")
                .secret(),
            "verysecrettestvalue"
        );
    }
}
