//! AWS KMS backend for [`crate::kms::AwsKmsKey`] (`internal/encryption/awskms`
//! in the Go baseline).
//!
//! [`new_key`] mirrors `awskms.NewKey`: `config.LoadDefaultConfig` with the
//! per-key `Region`/`CredentialsFile` overrides, then a `DescribeKey` probe
//! through [`AwsKmsKey::new`].

use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::Client;

use crate::kms::{AwsKmsKey, KmsDecrypted, KmsEncryptOutput, KmsMetadata, KmsTransport};
use crate::Error;

/// `schema.AWSKMSEncryptionKey`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AwsKmsConfig {
    pub key_id: String,
    pub region: String,
    pub credentials_file: String,
}

/// `awskms.NewKey`.
pub async fn new_key(config: &AwsKmsConfig) -> Result<AwsKmsKey, Error> {
    let sdk_config = load_config(config).await;
    let transport = Arc::new(AwsKmsClient {
        key_id: config.key_id.clone(),
        client: Client::new(&sdk_config),
    });
    AwsKmsKey::new(config.key_id.clone(), transport).await
}

/// `config.LoadDefaultConfig(..., awsConfigOptsForKeyConfig(...))`.
///
/// `WithRegion` replaces the default region chain; `WithSharedCredentialsFiles`
/// replaces the default credentials file but keeps `~/.aws/config`, which is
/// what [`ProfileFiles::include_default_config_file`] expresses.
async fn load_config(config: &AwsKmsConfig) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if !config.region.is_empty() {
        loader = loader.region(Region::new(config.region.clone()));
    }
    if !config.credentials_file.is_empty() {
        let files = EnvConfigFiles::builder()
            .include_default_config_file(true)
            .with_file(EnvConfigFileKind::Credentials, &config.credentials_file)
            .build();
        loader = loader.profile_files(files);
    }
    // `LoadDefaultConfig` resolves credentials lazily as well; a missing
    // credential is reported by the first request, not here.
    loader.load().await
}

/// [`KmsTransport`] over `aws_sdk_kms::Client`.
#[derive(Debug)]
pub struct AwsKmsClient {
    key_id: String,
    client: Client,
}

#[async_trait]
impl KmsTransport for AwsKmsClient {
    async fn describe(&self) -> Result<KmsMetadata, Error> {
        let out = self
            .client
            .describe_key()
            .key_id(&self.key_id)
            .send()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        // The baseline dereferences `KeyMetadata.Arn`/`KeyId` unconditionally.
        let metadata = out
            .key_metadata()
            .ok_or_else(|| Error::msg("key metadata is missing"))?;
        let version = metadata
            .arn()
            .ok_or_else(|| Error::msg("key metadata arn is missing"))?;
        Ok(KmsMetadata {
            name: metadata.key_id().to_owned(),
            version: version.to_owned(),
        })
    }

    async fn encrypt(&self, plaintext: &[u8]) -> Result<KmsEncryptOutput, Error> {
        let out = self
            .client
            .encrypt()
            .key_id(&self.key_id)
            .plaintext(Blob::new(plaintext.to_vec()))
            .send()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let key_name = out
            .key_id()
            .ok_or_else(|| Error::msg("encrypt response key id is missing"))?;
        let ciphertext = out
            .ciphertext_blob()
            .ok_or_else(|| Error::msg("encrypt response ciphertext is missing"))?;
        Ok(KmsEncryptOutput {
            key_name: key_name.to_owned(),
            ciphertext: ciphertext.as_ref().to_vec(),
            verified_plaintext_crc32c: true,
            ciphertext_crc32c: None,
        })
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<KmsDecrypted, Error> {
        let out = self
            .client
            .decrypt()
            .ciphertext_blob(Blob::new(ciphertext.to_vec()))
            .key_id(&self.key_id)
            .send()
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let plaintext = out
            .plaintext()
            .ok_or_else(|| Error::msg("decrypt response plaintext is missing"))?;
        Ok(KmsDecrypted {
            plaintext: plaintext.as_ref().to_vec(),
            plaintext_crc32c: None,
        })
    }
}
