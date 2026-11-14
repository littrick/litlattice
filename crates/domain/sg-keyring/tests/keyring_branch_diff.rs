//! M9.39d differential harness for `site config` KMS key parsing and the
//! `keyring.NewRing` branches. Go oracle:
//! `internal/encryption/keyring/zz_m939d_keyring_oracle_test.go` (UNTRACKED,
//! gated by `SG_M939D_KMS_WRAPPER_DIFF=1`). Only `noop` keys are constructed,
//! so no cloud client is created.

use serde_json::{json, Value};
use sg_config::{EncryptionKeyConfig, SiteConfiguration};

fn field(value: &Value, name: &str) -> String {
    value
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn describe_config(key: &Value) -> Value {
    let raw = format!(
        r#"{{"encryption.keys":{{"externalServiceKey":{}}}}}"#,
        serde_json::to_string(key).expect("serialize key")
    );
    let site = SiteConfiguration::parse(&raw).expect("parse site config");
    let keys = site.encryption_keys().expect("encryption keys");
    let (variant, fields) = match keys.external_service_key {
        Some(EncryptionKeyConfig::Awskms(config)) => (
            "awskms",
            json!({
                "credentials_file": config.credentials_file,
                "key_id": config.key_id,
                "region": config.region,
            }),
        ),
        Some(EncryptionKeyConfig::Cloudkms(config)) => (
            "cloudkms",
            json!({
                "credentials_file": config.credentials_file,
                "keyname": config.keyname,
            }),
        ),
        Some(EncryptionKeyConfig::Mounted(config)) => (
            "mounted",
            json!({
                "env_var_name": config.env_var_name,
                "filepath": config.filepath,
                "keyname": config.keyname,
                "version": config.version,
            }),
        ),
        Some(EncryptionKeyConfig::Noop) => ("noop", json!({})),
        Some(EncryptionKeyConfig::Unknown(_)) | None => ("unknown", json!({})),
    };
    json!({"fields": fields, "variant": variant})
}

async fn describe_ring(case: &Value) -> Value {
    let raw = match case.get("keys") {
        Some(keys) if !keys.is_null() => format!(
            r#"{{"encryption.keys":{}}}"#,
            serde_json::to_string(keys).expect("serialize keys")
        ),
        _ => "{}".to_owned(),
    };
    let site = SiteConfiguration::parse(&raw).expect("parse site config");
    let keyring = sg_keyring::init(&site).await.expect("init keyring");
    json!({
        "entries": {
            "batchChangesCredentialKey": keyring.batch_changes_credential_key().is_some(),
            "executorSecretKey": keyring.executor_secret_key().is_some(),
            "externalServiceKey": keyring.external_service_key().is_some(),
            "gitHubAppKey": keyring.github_app_key().is_some(),
            "outboundWebhookKey": keyring.outbound_webhook_key().is_some(),
            "userExternalAccountKey": keyring.user_external_account_key().is_some(),
            "webhookKey": keyring.webhook_key().is_some(),
            "webhookLogKey": keyring.webhook_log_key().is_some(),
        }
    })
}

#[tokio::test]
async fn keyring_branch_diff() {
    if std::env::var("SG_M939D_KMS_WRAPPER_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M939D_CORPUS").expect("M939D_CORPUS");
    let out_dir = std::env::var("M939D_OUT").expect("M939D_OUT");
    let raw = std::fs::read(&corpus_path).expect("read corpus");
    let corpus: Value = serde_json::from_slice(&raw).expect("parse corpus");
    let cases = corpus["cases"].as_array().expect("cases");
    for case in cases {
        let kind = field(case, "kind");
        let name = field(case, "name");
        let output = match kind.as_str() {
            "config" => describe_config(&case["key"]),
            "keyring" => describe_ring(case).await,
            _ => continue,
        };
        let bytes = serde_json::to_vec(&output).expect("serialize output");
        std::fs::write(format!("{out_dir}/{name}.json"), bytes).expect("write output");
    }
}
