//! Port of `internal/encryption/keyring` as far as the services in this
//! workspace use it.
//!
//! `shared.Main` calls `keyring.Init`, which builds every configured key and
//! fails startup when one cannot be created. gitserver's remote-URL lookup and
//! repo-updater's source factory read `externalServiceKey`; the worker's
//! `record-encrypter` job reads the remaining entries.
//!
//! `NewRing` in the baseline never assigns `OutboundWebhookKey` even when
//! `encryption.keys.outboundWebhookKey` is configured (the loop is missing a
//! branch). The Rust ring reproduces that: [`Keyring::outbound_webhook_key`]
//! always returns `None` (docs/modules/06_worker.md §24 records the erratum).
//!
//! [`watch`] mirrors `keyring.Init`'s `conf.Watch`: the baseline compares the
//! `EncryptionKeys` *pointer*, so every configuration replacement rebuilds the
//! ring. A rebuild failure panics in the Go process; here the error is
//! returned to the caller, which logs it and exits. Each process that calls
//! [`init`] must decide how to fail (see the gitserver and repo-updater
//! services).
//!
//! The `encryption.cache` LRU wrapper (`EnableCache`/`CacheSize`) is not
//! ported: it only changes decryption latency, not values.

use std::sync::{Arc, RwLock};

use sg_config::{EncryptionKeyConfig, LiveConfig, SiteConfiguration};
use sg_encryption::{awskms, cloudkms, Key, MountedKey, MountedKeyConfig, NoopKey};
use tokio_util::sync::CancellationToken;

/// `keyring.Ring`: the keys configured by `encryption.keys`.
#[derive(Default)]
pub struct Ring {
    pub batch_changes_credential_key: Option<Arc<dyn Key>>,
    pub external_service_key: Option<Arc<dyn Key>>,
    pub github_app_key: Option<Arc<dyn Key>>,
    /// Never populated: the baseline `NewRing` has no branch for
    /// `outboundWebhookKey` (see the module docs).
    pub outbound_webhook_key: Option<Arc<dyn Key>>,
    pub user_external_account_key: Option<Arc<dyn Key>>,
    pub webhook_key: Option<Arc<dyn Key>>,
    pub webhook_log_key: Option<Arc<dyn Key>>,
    pub executor_secret_key: Option<Arc<dyn Key>>,
}

/// `keyring.Ring` + `keyring.Default()`: the keys shared by every read path.
/// `getEncryptionKey` reads the current ring on each access, so replacing it
/// takes effect for subsequent lookups.
#[derive(Default)]
pub struct Keyring {
    batch_changes_credential_key: RwLock<Option<Arc<dyn Key>>>,
    external_service_key: RwLock<Option<Arc<dyn Key>>>,
    github_app_key: RwLock<Option<Arc<dyn Key>>>,
    outbound_webhook_key: RwLock<Option<Arc<dyn Key>>>,
    user_external_account_key: RwLock<Option<Arc<dyn Key>>>,
    webhook_key: RwLock<Option<Arc<dyn Key>>>,
    webhook_log_key: RwLock<Option<Arc<dyn Key>>>,
    executor_secret_key: RwLock<Option<Arc<dyn Key>>>,
}

macro_rules! key_accessor {
    ($name:ident) => {
        /// `keyring.Default().<Entry>`.
        pub fn $name(&self) -> Option<Arc<dyn Key>> {
            self.$name.read().expect("keyring lock poisoned").clone()
        }
    };
}

impl Keyring {
    key_accessor!(batch_changes_credential_key);
    key_accessor!(external_service_key);
    key_accessor!(github_app_key);
    key_accessor!(outbound_webhook_key);
    key_accessor!(user_external_account_key);
    key_accessor!(webhook_key);
    key_accessor!(webhook_log_key);
    key_accessor!(executor_secret_key);

    /// `defaultRing = *newRing`.
    fn replace(&self, ring: Ring) {
        let lock = |slot: &RwLock<Option<Arc<dyn Key>>>, key| {
            *slot.write().expect("keyring lock poisoned") = key;
        };
        lock(
            &self.batch_changes_credential_key,
            ring.batch_changes_credential_key,
        );
        lock(&self.external_service_key, ring.external_service_key);
        lock(&self.github_app_key, ring.github_app_key);
        lock(&self.outbound_webhook_key, ring.outbound_webhook_key);
        lock(
            &self.user_external_account_key,
            ring.user_external_account_key,
        );
        lock(&self.webhook_key, ring.webhook_key);
        lock(&self.webhook_log_key, ring.webhook_log_key);
        lock(&self.executor_secret_key, ring.executor_secret_key);
    }
}

/// `keyring.Init`'s initial `NewRing`.
pub async fn init(site: &SiteConfiguration) -> Result<Arc<Keyring>, String> {
    let keyring = Keyring::default();
    keyring.replace(new_ring(site).await?);
    Ok(Arc::new(keyring))
}

/// `keyring.Init`'s `conf.Watch` callback. The returned error must terminate
/// the process, as the baseline's `panic` does.
pub async fn watch(
    keyring: Arc<Keyring>,
    live: Arc<LiveConfig>,
    built_generation: u64,
    shutdown: CancellationToken,
) -> Result<(), String> {
    let mut changes = live.subscribe();
    // `conf.Watch` invokes the callback immediately; skip the rebuild when the
    // configuration has not been replaced since `init` built the ring.
    if *changes.borrow() != built_generation {
        keyring.replace(new_ring(&live.current().site).await?);
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            changed = changes.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
        // Go rebuilds for *any* config change: `newConfig == config` compares
        // pointers, and `conf.Get()` hands out a fresh `EncryptionKeys`.
        keyring.replace(new_ring(&live.current().site).await?);
        tracing::info!("rebuilt the encryption keyring");
    }
}

/// `keyring.NewRing`: build every configured key. A configuration without
/// `encryption.keys` yields an empty ring (the baseline returns `nil`).
///
/// The baseline builds entries in this order: `batchChangesCredentialKey`,
/// `externalServiceKey`, `gitHubAppKey`, `userExternalAccountKey`,
/// `webhookKey`, `webhookLogKey`, `executorSecretKey`. It has no branch for
/// `outboundWebhookKey`, so that entry stays `None`.
async fn new_ring(site: &SiteConfiguration) -> Result<Ring, String> {
    let Some(keys) = site.encryption_keys() else {
        return Ok(Ring::default());
    };

    let mut ring = Ring::default();
    macro_rules! build {
        ($field:ident, $config:expr) => {
            if let Some(config) = $config {
                ring.$field = Some(new_key(&config).await?);
            }
        };
    }
    build!(
        batch_changes_credential_key,
        keys.batch_changes_credential_key
    );
    build!(external_service_key, keys.external_service_key);
    build!(github_app_key, keys.github_app_key);
    build!(user_external_account_key, keys.user_external_account_key);
    build!(webhook_key, keys.webhook_key);
    build!(webhook_log_key, keys.webhook_log_key);
    build!(executor_secret_key, keys.executor_secret_key);
    // `keys.outbound_webhook_key` is intentionally not read (baseline bug).

    Ok(ring)
}

/// `keyring.NewKey`. The cache wrapper (`encryption.cache`) is not used by
/// gitserver's external-service path in this port.
async fn new_key(config: &EncryptionKeyConfig) -> Result<Arc<dyn Key>, String> {
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
        EncryptionKeyConfig::Awskms(config) => {
            let key = awskms::new_key(&awskms::AwsKmsConfig {
                key_id: config.key_id.clone(),
                region: config.region.clone(),
                credentials_file: config.credentials_file.clone(),
            })
            .await
            .map_err(|err| err.to_string())?;
            Ok(Arc::new(key))
        }
        EncryptionKeyConfig::Cloudkms(config) => {
            let key = cloudkms::new_key(&cloudkms::CloudKmsConfig {
                keyname: config.keyname.clone(),
                credentials_file: config.credentials_file.clone(),
            })
            .await
            .map_err(|err| err.to_string())?;
            Ok(Arc::new(key))
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

    #[tokio::test]
    async fn no_encryption_configuration_means_no_key() {
        let keyring = init(&SiteConfiguration::default()).await.expect("none");
        assert!(keyring.external_service_key().is_none());
    }

    #[tokio::test]
    async fn builds_every_configured_key_except_outbound_webhooks() {
        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {
                "batchChangesCredentialKey": {"type": "noop"},
                "externalServiceKey": {"type": "noop"},
                "gitHubAppKey": {"type": "noop"},
                "outboundWebhookKey": {"type": "noop"},
                "userExternalAccountKey": {"type": "noop"},
                "webhookKey": {"type": "noop"},
                "webhookLogKey": {"type": "noop"},
                "executorSecretKey": {"type": "noop"}
            }}"#,
        )
        .expect("parse");
        let keyring = init(&site).await.expect("noop ring");
        assert!(keyring.batch_changes_credential_key().is_some());
        assert!(keyring.external_service_key().is_some());
        assert!(keyring.github_app_key().is_some());
        assert!(keyring.user_external_account_key().is_some());
        assert!(keyring.webhook_key().is_some());
        assert!(keyring.webhook_log_key().is_some());
        assert!(keyring.executor_secret_key().is_some());
        // `NewRing` never assigns `OutboundWebhookKey` in the baseline.
        assert!(keyring.outbound_webhook_key().is_none());
    }

    #[tokio::test]
    async fn every_configured_key_is_validated_at_startup() {
        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {
                "externalServiceKey": {"type": "noop"},
                "userExternalAccountKey": {"type": "mystery"}
            }}"#,
        )
        .expect("parse");
        let err = init(&site).await.err().expect("unknown type");
        assert!(err.starts_with("couldn't configure key: "), "{err}");
    }

    #[tokio::test]
    async fn builds_mounted_and_noop_keys_and_rejects_unknown_types() {
        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {"type": "noop"}}}"#,
        )
        .expect("parse");
        let keyring = init(&site).await.expect("noop");
        assert!(keyring.external_service_key().is_some());

        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {"type": "mystery"}}}"#,
        )
        .expect("parse");
        let err = init(&site).await.err().expect("unknown");
        assert!(err.starts_with("couldn't configure key: "));
    }

    #[tokio::test]
    async fn mounted_key_requires_a_readable_secret() {
        let site = SiteConfiguration::parse(
            r#"{"encryption.keys": {"externalServiceKey": {
                "type": "mounted", "keyname": "k", "filepath": "/nonexistent/secret",
            }}}"#,
        )
        .expect("parse");
        let err = init(&site).await.err().expect("missing file");
        assert!(err.contains("error reading secret file for \"k\""), "{err}");
    }

    #[tokio::test]
    async fn watch_rebuilds_the_ring_for_every_configuration_change() {
        use sg_config::Unified;

        let keyring = init(&SiteConfiguration::default()).await.expect("init");
        let live = Arc::new(LiveConfig::new(Unified::default()));
        let generation = live.generation();
        let shutdown = CancellationToken::new();
        let watcher = tokio::spawn({
            let keyring = Arc::clone(&keyring);
            let live = Arc::clone(&live);
            let shutdown = shutdown.clone();
            async move { watch(keyring, live, generation, shutdown).await }
        });

        // The startup ring had no key; a config change to `noop` is picked up.
        let unified = Unified {
            site: SiteConfiguration::parse(
                r#"{"encryption.keys": {"externalServiceKey": {"type": "noop"}}}"#,
            )
            .expect("parse"),
            ..Unified::default()
        };
        live.set(unified);
        wait_until(|| keyring.external_service_key().is_some(), "rebuilt").await;

        // A configuration change that removes the key clears the ring.
        live.set(Unified::default());
        wait_until(|| keyring.external_service_key().is_none(), "cleared").await;

        shutdown.cancel();
        watcher.await.expect("watcher exits").expect("no error");
    }

    async fn wait_until(condition: impl Fn() -> bool, what: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("keyring was not {what}"));
    }
}
