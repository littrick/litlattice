//! Google Cloud KMS backend for [`crate::kms::CloudKmsKey`]
//! (`internal/encryption/cloudkms` in the Go baseline).
//!
//! [`new_key`] mirrors `cloudkms.NewKey`: build a `KeyManagementClient` with an
//! optional `WithCredentialsFile`, then probe it with `GetCryptoKey`. The
//! service's CRC32C fields are carried through to the key, which validates
//! them exactly like the baseline.

use std::sync::Arc;

use async_trait::async_trait;
use google_cloud_auth::credentials::service_account;
use google_cloud_kms_v1::client::KeyManagementService;

use crate::kms::{crc32c, CloudKmsKey, KmsDecrypted, KmsEncryptOutput, KmsMetadata, KmsTransport};
use crate::Error;

/// `schema.CloudKMSEncryptionKey`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudKmsConfig {
    pub keyname: String,
    pub credentials_file: String,
}

/// `cloudkms.NewKey`.
pub async fn new_key(config: &CloudKmsConfig) -> Result<CloudKmsKey, Error> {
    let mut builder = KeyManagementService::builder();
    if !config.credentials_file.is_empty() {
        // `option.WithCredentialsFile`: a service-account key JSON. ADC
        // (`GOOGLE_APPLICATION_CREDENTIALS`/metadata server) is used otherwise.
        let data = std::fs::read(&config.credentials_file).map_err(|err| {
            Error::msg(format!(
                "reading credentials file {}: {err}",
                go_quote(&config.credentials_file)
            ))
        })?;
        let key: serde_json::Value = serde_json::from_slice(&data).map_err(|err| {
            Error::msg(format!(
                "parsing credentials file {}: {err}",
                go_quote(&config.credentials_file)
            ))
        })?;
        let credentials = service_account::Builder::new(key)
            .build()
            .map_err(|err| Error::msg(format!("creating credentials: {err}")))?;
        builder = builder.with_credentials(credentials);
    }
    let client = builder
        .build()
        .await
        .map_err(|err| Error::msg(err.to_string()))?;

    let transport = Arc::new(CloudKmsClient {
        name: config.keyname.clone(),
        client,
    });
    CloudKmsKey::new(config.keyname.clone(), transport).await
}

/// [`KmsTransport`] over `google_cloud_kms_v1::client::KeyManagementService`.
#[derive(Debug)]
pub struct CloudKmsClient {
    name: String,
    client: KeyManagementService,
}

#[async_trait]
impl KmsTransport for CloudKmsClient {
    async fn describe(&self) -> Result<KmsMetadata, Error> {
        let key = self
            .client
            .get_crypto_key()
            .set_name(&self.name)
            .send()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        // The baseline dereferences `CryptoKey.Primary` unconditionally.
        let primary = key
            .primary
            .as_ref()
            .ok_or_else(|| Error::msg("crypto key primary version is missing"))?;
        Ok(KmsMetadata {
            name: key.name.clone(),
            version: primary.name.clone(),
        })
    }

    async fn encrypt(&self, plaintext: &[u8]) -> Result<KmsEncryptOutput, Error> {
        let plaintext_crc32c = i64::from(crc32c(plaintext));
        let res = self
            .client
            .encrypt()
            .set_name(&self.name)
            .set_plaintext(plaintext.to_vec())
            .set_plaintext_crc32c(plaintext_crc32c)
            .send()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        Ok(KmsEncryptOutput {
            key_name: res.name,
            ciphertext: res.ciphertext.to_vec(),
            verified_plaintext_crc32c: res.verified_plaintext_crc32c,
            ciphertext_crc32c: res.ciphertext_crc32c,
        })
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<KmsDecrypted, Error> {
        let res = self
            .client
            .decrypt()
            .set_name(&self.name)
            .set_ciphertext(ciphertext.to_vec())
            .send()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        Ok(KmsDecrypted {
            plaintext: res.plaintext.to_vec(),
            plaintext_crc32c: res.plaintext_crc32c,
        })
    }
}

/// Go's `%q` for paths reported in errors.
fn go_quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}
