//! Port of `internal/encryption/keyring` as far as gitserver uses it.
//!
//! `shared.Main` calls `keyring.Init`, which builds every configured key and
//! fails startup when one cannot be created. gitserver only reads
//! `externalServiceKey` (via `database.ExternalServices.getEncryptionKey`), so
//! only that entry is constructed.
//!
//! [`watch`] mirrors `keyring.Init`'s `conf.Watch`: the baseline compares the
//! `EncryptionKeys` *pointer*, so every configuration replacement rebuilds the
//! ring. A rebuild failure panics in the Go process; here the error is
//! returned to the service, which logs it and exits (see `lib.rs`).

use std::sync::{Arc, RwLock};

use sg_config::{EncryptionKeyConfig, LiveConfig, SiteConfiguration};
use sg_encryption::{awskms, cloudkms, Key, MountedKey, MountedKeyConfig, NoopKey};
use tokio_util::sync::CancellationToken;

/// `keyring.Ring` + `keyring.Default()`: the keys shared by every read path.
/// `getEncryptionKey` reads the current ring on each access, so replacing it
/// takes effect for subsequent external-service lookups.
#[derive(Default)]
pub struct Keyring {
    external_service_key: RwLock<Option<Arc<dyn Key>>>,
}

impl Keyring {
    /// `keyring.Default().ExternalServiceKey`.
    pub fn external_service_key(&self) -> Option<Arc<dyn Key>> {
        self.external_service_key
            .read()
            .expect("keyring lock poisoned")
            .clone()
    }

    /// `defaultRing = *newRing`.
    fn replace(&self, key: Option<Arc<dyn Key>>) {
        *self
            .external_service_key
            .write()
            .expect("keyring lock poisoned") = key;
    }
}

/// `keyring.Init`'s initial `NewRing`: build the `externalServiceKey` from
/// `encryption.keys.externalServiceKey`. `Ok(None)` when the site config does
/// not configure the keyring.
pub async fn init(site: &SiteConfiguration) -> Result<Arc<Keyring>, String> {
    let keyring = Keyring::default();
    keyring.replace(external_service_key(site).await?);
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
        keyring.replace(external_service_key(&live.current().site).await?);
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
        keyring.replace(external_service_key(&live.current().site).await?);
        tracing::info!("rebuilt the encryption keyring");
    }
}

/// The `externalServiceKey` entry of `encryption.keys`.
async fn external_service_key(site: &SiteConfiguration) -> Result<Option<Arc<dyn Key>>, String> {
    let Some(keys) = site.encryption_keys() else {
        return Ok(None);
    };
    let Some(key) = keys.external_service_key else {
        return Ok(None);
    };
    Ok(Some(new_key(&key).await?))
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
