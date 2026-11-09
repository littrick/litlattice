#![forbid(unsafe_code)]

//! Port of `internal/conf/store.go`: the frontend process' in-memory site
//! configuration store.
//!
//! The store keeps the last successfully parsed configuration, the last raw
//! configuration it was handed, and an optional mock. `LastValid`/`Raw` block
//! until it has been initialized by the first successful `MaybeUpdate` or by
//! `Mock`. `MaybeUpdate` compares the raw configuration without its `id`
//! (`RawUnified.Equal`), records the new raw value *before* parsing (so a
//! parse failure still advances `Raw` while `LastValid` stays on the old
//! value), and only initializes the store on a successful parse.
//!
//! Not ported here (see docs/overviews/m9_8ee): the `checkDeadlock` panic
//! guard (there is no configuration-server-startup channel until the
//! frontend config service slice) and the typed `json.Marshal` of a mock's
//! site config (`Raw` serializes the stored JSON value instead).

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::{ConfigError, RawUnified, Unified};

/// `conf.updateResult`: the previous and new configuration snapshots.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateResult {
    /// Whether the store's `LastValid` cache was updated.
    pub changed: bool,
    /// The configuration before the update (nil before initialization).
    pub old: Option<Arc<Unified>>,
    /// The configuration after the update.
    pub new: Option<Arc<Unified>>,
}

/// The two failure modes of `store.MaybeUpdate`.
#[derive(Debug)]
pub enum StoreError {
    /// `invalid site configuration (empty string)`.
    EmptySite,
    /// `when parsing rawConfig during update: <err>`.
    Parse(ConfigError),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::EmptySite => {
                write!(f, "invalid site configuration (empty string)")
            }
            StoreError::Parse(err) => {
                write!(f, "when parsing rawConfig during update: {err}")
            }
        }
    }
}

impl std::error::Error for StoreError {}

#[derive(Debug, Default)]
struct Inner {
    last_valid: Option<Arc<Unified>>,
    mock: Option<Arc<Unified>>,
    raw: Option<RawUnified>,
    initialized: bool,
}

/// `internal/conf.store`: the threadsafe in-memory configuration cache.
#[derive(Debug, Default)]
pub struct ConfigStore {
    inner: Mutex<Inner>,
    ready: Condvar,
}

impl ConfigStore {
    /// `newStore`: the store blocks readers until the first update or mock.
    pub fn new() -> Self {
        Self::default()
    }

    /// `store.LastValid`: the last valid configuration, or the mock.
    pub fn last_valid(&self) -> Option<Arc<Unified>> {
        let inner = self.wait();
        inner.mock.clone().or_else(|| inner.last_valid.clone())
    }

    /// `store.Raw`: the last raw configuration (or the mock's marshaled site
    /// config). A failed mock serialization degrades to the zero value, like
    /// the baseline.
    pub fn raw(&self) -> RawUnified {
        let inner = self.wait();
        if let Some(mock) = &inner.mock {
            return match serde_json::to_string(mock.site.raw()) {
                Ok(site) => RawUnified {
                    site,
                    service_connections: mock.service_connections.clone(),
                    ..RawUnified::default()
                },
                Err(_) => RawUnified::default(),
            };
        }
        inner.raw.clone().unwrap_or_default()
    }

    /// `store.Mock`: use this configuration for `LastValid`/`Raw` until it is
    /// replaced; also initializes the store.
    pub fn mock(&self, mockery: Unified) {
        let mut inner = self.lock();
        inner.mock = Some(Arc::new(mockery));
        Self::initialize(&mut inner, &self.ready);
    }

    /// `store.MaybeUpdate`. The returned result always carries the previous
    /// and new snapshots; the error (if any) means the cache was not updated.
    pub fn maybe_update(&self, raw_config: &RawUnified) -> (UpdateResult, Result<(), StoreError>) {
        let mut inner = self.lock();

        let mut result = UpdateResult {
            changed: false,
            old: inner.last_valid.clone(),
            new: inner.last_valid.clone(),
        };

        if raw_config.site.is_empty() {
            return (result, Err(StoreError::EmptySite));
        }
        if inner.raw.as_ref().is_some_and(|raw| raw.equal(raw_config)) {
            return (result, Ok(()));
        }

        // The baseline records the raw value before parsing: a parse failure
        // leaves `Raw` advanced while `LastValid` keeps the old config.
        inner.raw = Some(raw_config.clone());

        let new_config = match Unified::parse(raw_config) {
            Ok(config) => Arc::new(config),
            Err(err) => return (result, Err(StoreError::Parse(err))),
        };

        result.changed = true;
        result.new = Some(new_config.clone());
        inner.last_valid = Some(new_config);
        Self::initialize(&mut inner, &self.ready);
        (result, Ok(()))
    }

    /// `store.WaitUntilInitialized`: block until `MaybeUpdate` succeeded or
    /// `Mock` was called.
    pub fn wait_until_initialized(&self) {
        let _inner = self.wait();
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("config store lock poisoned")
    }

    fn wait(&self) -> MutexGuard<'_, Inner> {
        let mut inner = self.lock();
        while !inner.initialized {
            inner = self.ready.wait(inner).expect("config store lock poisoned");
        }
        inner
    }

    fn initialize(inner: &mut Inner, ready: &Condvar) {
        if !inner.initialized {
            inner.initialized = true;
            ready.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServiceConnections;
    use std::sync::mpsc;
    use std::time::Duration;

    fn raw(site: &str) -> RawUnified {
        RawUnified {
            site: site.to_owned(),
            ..RawUnified::default()
        }
    }

    fn providers(store: &ConfigStore) -> usize {
        store
            .last_valid()
            .expect("initialized")
            .site
            .raw()
            .get("auth.providers")
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len)
    }

    #[test]
    fn first_update_initializes_and_reports_the_snapshots() {
        let store = ConfigStore::new();
        let (result, error) = store.maybe_update(&RawUnified {
            site: r#"{"auth.providers":[{"type":"builtin"}]}"#.to_owned(),
            service_connections: ServiceConnections {
                git_servers: vec!["g1".to_owned()],
                postgres_dsn: "pg".to_owned(),
                ..ServiceConnections::default()
            },
            ..RawUnified::default()
        });
        assert!(error.is_ok());
        assert!(result.changed);
        assert!(result.old.is_none());
        assert!(result.new.is_some());
        assert_eq!(providers(&store), 1);
        let raw = store.raw();
        assert_eq!(raw.service_connections.git_servers, ["g1"]);
        assert_eq!(raw.service_connections.postgres_dsn, "pg");
        assert_eq!(raw.site, r#"{"auth.providers":[{"type":"builtin"}]}"#);
    }

    #[test]
    fn empty_site_is_rejected_before_raw_is_recorded() {
        let store = ConfigStore::new();
        let (result, error) = store.maybe_update(&raw(""));
        assert_eq!(
            error.unwrap_err().to_string(),
            "invalid site configuration (empty string)"
        );
        assert!(!result.changed);
        assert_eq!(result.old, None);
        assert_eq!(result.new, None);

        // The store is still uninitialized; a valid update initializes it.
        let (result, error) = store.maybe_update(&raw("{}"));
        assert!(error.is_ok());
        assert!(result.changed);
        assert!(store.last_valid().is_some());
    }

    #[test]
    fn equal_raw_ignores_the_id_and_detects_connection_changes() {
        let store = ConfigStore::new();
        let mut first = raw("{}");
        first.id = 1;
        let _ = store.maybe_update(&first);

        let mut same = raw("{}");
        same.id = 2;
        let (result, error) = store.maybe_update(&same);
        assert!(error.is_ok());
        assert!(!result.changed);
        assert_eq!(store.raw().id, 1, "the ignored update must not replace raw");

        let mut connections = same.clone();
        connections.service_connections.git_servers = vec!["g".to_owned()];
        let (result, error) = store.maybe_update(&connections);
        assert!(error.is_ok());
        assert!(result.changed);
        assert_eq!(store.raw().id, 2);
    }

    #[test]
    fn parse_failure_keeps_last_valid_but_advances_raw() {
        let store = ConfigStore::new();
        let _ = store.maybe_update(&raw(r#"{"auth.providers":[{"type":"builtin"}]}"#));

        let (result, error) = store.maybe_update(&raw("{"));
        assert!(matches!(error, Err(StoreError::Parse(_))));
        assert!(!result.changed);
        assert_eq!(providers(&store), 1);
        assert_eq!(store.raw().site, "{");

        // Repeating the same invalid raw value is "equal" and error-free, like
        // the baseline's Equal check happening before parsing.
        let (result, error) = store.maybe_update(&raw("{"));
        assert!(error.is_ok());
        assert!(!result.changed);
    }

    #[test]
    fn an_empty_site_after_a_parse_failure_does_not_clear_raw() {
        let store = ConfigStore::new();
        let _ = store.maybe_update(&raw("{}"));
        let _ = store.maybe_update(&raw("{"));
        let (_, error) = store.maybe_update(&raw(""));
        assert!(matches!(error, Err(StoreError::EmptySite)));
        assert_eq!(store.raw().site, "{");
    }

    #[test]
    fn mock_takes_precedence_over_last_valid_and_raw() {
        let store = ConfigStore::new();
        let _ = store.maybe_update(&RawUnified {
            site: r#"{"externalURL":"https://db.example"}"#.to_owned(),
            service_connections: ServiceConnections {
                postgres_dsn: "db".to_owned(),
                ..ServiceConnections::default()
            },
            ..RawUnified::default()
        });
        store.mock(Unified {
            site: crate::SiteConfiguration::parse(r#"{"externalURL":"https://mock.example"}"#)
                .expect("site"),
            service_connections: ServiceConnections {
                postgres_dsn: "mock".to_owned(),
                ..ServiceConnections::default()
            },
        });

        assert!(providers(&store) == 0);
        let last = store.last_valid().expect("initialized");
        assert_eq!(
            last.site.raw().get("externalURL").and_then(|v| v.as_str()),
            Some("https://mock.example")
        );
        let raw = store.raw();
        assert_eq!(raw.service_connections.postgres_dsn, "mock");
        assert_eq!(raw.site, r#"{"externalURL":"https://mock.example"}"#);
    }

    #[test]
    fn readers_wait_until_initialized() {
        let store = Arc::new(ConfigStore::new());
        let reader = Arc::clone(&store);
        let (sender, receiver) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            sender.send(reader.last_valid().is_some()).expect("send");
        });

        assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
        let _ = store.maybe_update(&raw("{}"));
        assert!(receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("initialized"));
        handle.join().expect("join");
    }
}
