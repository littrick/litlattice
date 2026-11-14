//! M9.39d differential harness: AWS/Cloud KMS wrapper against the Go baseline
//! (`internal/encryption/awskms`, `internal/encryption/cloudkms`).
//!
//! Both sides drive the real key types with an in-process mock transport that
//! never contacts a cloud service:
//!
//! * `describe` -> the configured name/version,
//! * `encrypt(plaintext)` -> `b"kms:" ++ plaintext`,
//! * `decrypt(ciphertext)` -> `ciphertext` without the `kms:` prefix,
//!
//! optionally corrupting the CRC fields Cloud KMS validates. Because envelope
//! keys and AES nonces are random, the harness compares the *structure* of the
//! produced wrapper (mechanism, key name, lengths), the decrypted plaintext,
//! the `KeyVersion.JSON` string, and the exact baseline error texts, not the
//! random ciphertext bytes.
//!
//! The Go oracle is `internal/encryption/{awskms,cloudkms}/zz_m939d_*_test.go`
//! (UNTRACKED, gated by `SG_M939D_KMS_WRAPPER_DIFF=1`). Corpus:
//! `test_ws/p9-go/data/kms_wrapper/corpus.json`.

use std::sync::{Arc, Mutex};

use aes_gcm::aead::Aead as _;
use aes_gcm::{Aes256Gcm, KeyInit as _};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sg_encryption::kms::{KmsDecrypted, KmsEncryptOutput, KmsMetadata};
use sg_encryption::{
    AwsKmsKey, CloudKmsKey, Error, Key, KmsTransport, StorableEncryptedKey,
};

const DEFAULT_AWS_KEY_ID: &str = "4b739277-5a93-4551-b71c-99608c9c805d";

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct Case {
    name: String,
    kind: String,
    op: String,
    #[serde(default)]
    key_id: String,
    #[serde(default)]
    describe_name: String,
    #[serde(default)]
    describe_version: String,
    #[serde(default)]
    plaintext: String,
    #[serde(default)]
    repeat_text: String,
    #[serde(default)]
    repeat_count: usize,
    #[serde(default)]
    corrupt: String,
    #[serde(default)]
    mechanism: String,
    #[serde(default)]
    key_name: String,
    #[serde(default)]
    wrapped_key_b64: String,
    #[serde(default)]
    ciphertext_b64: String,
    #[serde(default)]
    nonce_b64: String,
    #[serde(default)]
    legacy_plaintext: String,
    #[serde(default)]
    data_key_hex: String,
    #[serde(default)]
    nonce_hex: String,
}

fn crc32c(data: &[u8]) -> u32 {
    static CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
    CRC.checksum(data)
}

/// The shared in-memory KMS service.
#[derive(Debug)]
struct MockTransport {
    name: String,
    version: String,
    corrupt: String,
    calls: Mutex<Vec<String>>,
}

impl MockTransport {
    fn new(name: String, version: String, corrupt: String) -> Self {
        Self {
            name,
            version,
            corrupt,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("calls lock").push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls lock").clone()
    }
}

fn wrap(plaintext: &[u8]) -> Vec<u8> {
    let mut ciphertext = b"kms:".to_vec();
    ciphertext.extend_from_slice(plaintext);
    ciphertext
}

fn unwrap(ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
    ciphertext
        .strip_prefix(b"kms:")
        .map(<[u8]>::to_vec)
        .ok_or_else(|| Error::msg("unwrapping key: invalid ciphertext"))
}

#[async_trait]
impl KmsTransport for MockTransport {
    async fn describe(&self) -> Result<KmsMetadata, Error> {
        self.record("describe".to_owned());
        Ok(KmsMetadata {
            name: self.name.clone(),
            version: self.version.clone(),
        })
    }

    async fn encrypt(&self, plaintext: &[u8]) -> Result<KmsEncryptOutput, Error> {
        self.record(format!("encrypt:{}", plaintext.len()));
        let ciphertext = wrap(plaintext);
        let mut crc = i64::from(crc32c(&ciphertext));
        if self.corrupt == "ciphertext_crc" {
            crc += 1;
        }
        Ok(KmsEncryptOutput {
            key_name: self.name.clone(),
            ciphertext,
            verified_plaintext_crc32c: self.corrupt != "verified_false",
            ciphertext_crc32c: Some(crc),
        })
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<KmsDecrypted, Error> {
        self.record(format!("decrypt:{}", ciphertext.len()));
        let plaintext = unwrap(ciphertext)?;
        let mut crc = i64::from(crc32c(&plaintext));
        if self.corrupt == "plaintext_crc" {
            crc += 1;
        }
        Ok(KmsDecrypted {
            plaintext,
            plaintext_crc32c: Some(crc),
        })
    }
}

fn case_plaintext(case: &Case) -> Vec<u8> {
    if case.repeat_count > 0 {
        case.repeat_text.repeat(case.repeat_count).into_bytes()
    } else {
        case.plaintext.as_bytes().to_vec()
    }
}

fn key_id(case: &Case) -> String {
    if case.key_id.is_empty() {
        DEFAULT_AWS_KEY_ID.to_owned()
    } else {
        case.key_id.clone()
    }
}

fn describe_name(case: &Case) -> String {
    if case.describe_name.is_empty() {
        key_id(case)
    } else {
        case.describe_name.clone()
    }
}

fn describe_version(case: &Case) -> String {
    if case.describe_version.is_empty() {
        "version/1".to_owned()
    } else {
        case.describe_version.clone()
    }
}

fn build_key(case: &Case, transport: Arc<MockTransport>) -> Box<dyn Key> {
    match case.kind.as_str() {
        "aws" => Box::new(AwsKmsKey::with_transport(key_id(case), transport)),
        "cloud" => Box::new(CloudKmsKey::with_transport(key_id(case), transport)),
        other => panic!("unsupported KMS kind {other}"),
    }
}

fn decode(value: &str) -> Vec<u8> {
    STANDARD.decode(value.as_bytes()).unwrap_or_default()
}

#[derive(Serialize)]
struct AwsLegacy<'a> {
    #[serde(rename = "Key")]
    key: &'a str,
    #[serde(rename = "Nonce")]
    nonce: &'a str,
    #[serde(rename = "Ciphertext")]
    ciphertext: &'a str,
}

#[derive(Serialize)]
struct CloudLegacy<'a> {
    #[serde(rename = "KeyName")]
    key_name: &'a str,
    #[serde(rename = "Ciphertext")]
    ciphertext: &'a str,
    #[serde(rename = "Checksum")]
    checksum: u32,
}

fn hex_decode(value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars: Vec<char> = value.chars().collect();
    for pair in chars.chunks(2) {
        let text: String = pair.iter().collect();
        bytes.push(u8::from_str_radix(&text, 16).expect("hex"));
    }
    bytes
}

async fn run_case(case: &Case) -> Value {
    let transport = Arc::new(MockTransport::new(
        describe_name(case),
        describe_version(case),
        case.corrupt.clone(),
    ));
    let key = build_key(case, transport.clone());

    let mut encrypt_mechanism = String::new();
    let mut encrypt_key_name = String::new();
    let mut encrypt_wrapped_len = 0_usize;
    let mut encrypt_ciphertext_len = 0_usize;
    let mut encrypt_nonce_len = 0_usize;
    let mut encrypt_error = String::new();
    let mut decrypt_plaintext = String::new();
    let mut decrypt_error = String::new();
    let mut legacy = String::new();
    let mut version = String::new();

    match case.op.as_str() {
        "roundtrip" => {
            let plaintext = case_plaintext(case);
            match key.encrypt(&plaintext).await {
                Ok(ciphertext) => {
                    let storable =
                        StorableEncryptedKey::from_ciphertext(&ciphertext).expect("wrapper");
                    encrypt_mechanism = storable.mechanism;
                    encrypt_key_name = storable.key_name;
                    encrypt_wrapped_len = storable.wrapped_key.len();
                    encrypt_ciphertext_len = storable.ciphertext.len();
                    encrypt_nonce_len = storable.nonce.len();
                    match key.decrypt(&ciphertext).await {
                        Ok(secret) => decrypt_plaintext = secret.secret().to_owned(),
                        Err(err) => decrypt_error = err.to_string(),
                    }
                }
                Err(err) => encrypt_error = err.to_string(),
            }
            match key.version().await {
                Ok(meta) => version = meta.json(),
                Err(err) => version = format!("error: {err}"),
            }
        }
        "encrypt" => {
            let plaintext = case_plaintext(case);
            match key.encrypt(&plaintext).await {
                Ok(ciphertext) => {
                    let storable =
                        StorableEncryptedKey::from_ciphertext(&ciphertext).expect("wrapper");
                    encrypt_mechanism = storable.mechanism;
                    encrypt_key_name = storable.key_name;
                    encrypt_wrapped_len = storable.wrapped_key.len();
                    encrypt_ciphertext_len = storable.ciphertext.len();
                    encrypt_nonce_len = storable.nonce.len();
                }
                Err(err) => encrypt_error = err.to_string(),
            }
        }
        "version" => match key.version().await {
            Ok(meta) => version = meta.json(),
            Err(err) => version = format!("error: {err}"),
        },
        "decrypt_wrapper" => {
            let storable = StorableEncryptedKey {
                mechanism: case.mechanism.clone(),
                key_name: case.key_name.clone(),
                wrapped_key: decode(&case.wrapped_key_b64),
                ciphertext: decode(&case.ciphertext_b64),
                nonce: decode(&case.nonce_b64),
            };
            let ciphertext = storable.serialize().expect("serialize");
            match key.decrypt(&ciphertext).await {
                Ok(secret) => decrypt_plaintext = secret.secret().to_owned(),
                Err(err) => decrypt_error = err.to_string(),
            }
        }
        "legacy_aws" => {
            let data_key = hex_decode(&case.data_key_hex);
            let nonce = hex_decode(&case.nonce_hex);
            let cipher = Aes256Gcm::new_from_slice(&data_key).expect("aes key");
            let aes_ciphertext = cipher
                .encrypt(aes_gcm::Nonce::from_slice(&nonce), case.legacy_plaintext.as_bytes())
                .expect("aes encrypt");
            let mut wrapped = b"kms:".to_vec();
            wrapped.extend_from_slice(&data_key);
            let key_b64 = STANDARD.encode(&wrapped);
            let nonce_b64 = STANDARD.encode(&nonce);
            let ciphertext_b64 = STANDARD.encode(&aes_ciphertext);
            let json = serde_json::to_vec(&AwsLegacy {
                key: &key_b64,
                nonce: &nonce_b64,
                ciphertext: &ciphertext_b64,
            })
            .expect("legacy json");
            legacy = STANDARD.encode(&json);
            match key.decrypt(legacy.as_bytes()).await {
                Ok(secret) => decrypt_plaintext = secret.secret().to_owned(),
                Err(err) => decrypt_error = err.to_string(),
            }
        }
        "legacy_cloud" => {
            let ciphertext = wrap(case.legacy_plaintext.as_bytes());
            let checksum = crc32c(case.legacy_plaintext.as_bytes());
            let ciphertext_b64 = STANDARD.encode(&ciphertext);
            let json = serde_json::to_vec(&CloudLegacy {
                key_name: &key_id(case),
                ciphertext: &ciphertext_b64,
                checksum,
            })
            .expect("legacy json");
            legacy = STANDARD.encode(&json);
            match key.decrypt(legacy.as_bytes()).await {
                Ok(secret) => decrypt_plaintext = secret.secret().to_owned(),
                Err(err) => decrypt_error = err.to_string(),
            }
        }
        other => panic!("unsupported op {other}"),
    }

    json!({
        "calls": transport.calls(),
        "decrypt": {
            "error": decrypt_error,
            "plaintext": decrypt_plaintext,
        },
        "encrypt": {
            "ciphertext_len": encrypt_ciphertext_len,
            "error": encrypt_error,
            "key_name": encrypt_key_name,
            "mechanism": encrypt_mechanism,
            "nonce_len": encrypt_nonce_len,
            "wrapped_len": encrypt_wrapped_len,
        },
        "legacy": legacy,
        "version": version,
    })
}

#[tokio::test]
async fn kms_wrapper_diff() {
    if std::env::var("SG_M939D_KMS_WRAPPER_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M939D_CORPUS").expect("M939D_CORPUS");
    let out_dir = std::env::var("M939D_OUT").expect("M939D_OUT");
    let raw = std::fs::read(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_slice(&raw).expect("parse corpus");

    for case in &corpus.cases {
        if case.kind != "aws" && case.kind != "cloud" {
            continue;
        }
        let output = run_case(case).await;
        let bytes = serde_json::to_vec(&output).expect("serialize output");
        std::fs::write(format!("{out_dir}/{}.json", case.name), bytes).expect("write output");
    }
}
