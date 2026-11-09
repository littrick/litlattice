#![forbid(unsafe_code)]

//! Port of the frontend's configuration-server startup
//! (`conf.InitConfigurationServerFrontendOnly` in
//! `internal/conf/conf.go:215-247`) and the passthrough update loop
//! (`client.continuouslyUpdate` in `internal/conf/client.go:240-360`).
//!
//! [`FrontendConfigurationServer`] wires the DB source (M9.8ei/ej) into the
//! async [`sg_config::server::Server`] behind the 2.5s
//! [`sg_config::server::CachedConfigurationSource`], keeps a
//! [`sg_config::store::ConfigStore`] and a [`sg_config::LiveConfig`], and
//! runs two structured tasks until shutdown:
//!
//! * the restart watcher (`Server::run`), which marks the server as needing a
//!   restart when a changed option is in `requireRestart`;
//! * the update loop, which reads the *raw* source on every `sourceUpdates`
//!   signal or poll tick, updates the store, and reports back to
//!   `Server::write` (the baseline's `sourceWrites` handshake).
//!
//! Errata (see docs/overviews/m9_8ek_conf_server_wiring_overview.md): no
//! client/remote mode selection, no passthrough/internal-API fetch for other
//! processes, no `SITE_CONFIG_ESCAPE_HATCH_PATH` worker, no random poll
//! jitter (fixed 2.5s), and the first read is done before the watcher starts
//! so a failure fails startup (the baseline logs and retries). sqlx ignores
//! the DSN's `timezone` parameter; timestamps stay correct as instants.

use std::sync::Arc;
use std::time::Duration;

use sg_config::server::{CachedConfigurationSource, ConfigurationSource, Server};
use sg_config::store::ConfigStore;
use sg_config::{LiveConfig, RawUnified, Unified};
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{configuration_source::DbConfigurationSource, service_connections};

/// `CachedConfigurationSource<DbConfigurationSource>`: the server's source.
pub type CachedDbSource = CachedConfigurationSource<DbConfigurationSource>;
/// `conf.Server` over the cached DB source.
pub type DbConfigServer = Server<CachedDbSource>;

/// Fixed stand-in for `client.continuouslyUpdate`'s `rand.Int63n(5s)` jitter.
pub const UPDATE_INTERVAL: Duration = Duration::from_millis(2500);

/// The frontend process' configuration server and its background tasks.
pub struct FrontendConfigurationServer {
    server: Arc<DbConfigServer>,
    store: Arc<ConfigStore>,
    live: Arc<LiveConfig>,
    shutdown: CancellationToken,
    tasks: JoinSet<()>,
}

impl FrontendConfigurationServer {
    /// Build a pool from `serviceConnections().PostgresDSN` (the frontend
    /// schema DSN).
    pub async fn connect_frontend_pool() -> Result<PgPool, String> {
        let connections = service_connections::service_connections()?;
        PgPool::connect(&connections.postgres_dsn)
            .await
            .map_err(|err| format!("connect to database: {err}"))
    }

    /// Start from an existing pool.
    pub async fn start_with_pool(
        pool: PgPool,
        shutdown: CancellationToken,
    ) -> Result<Self, String> {
        let source = DbConfigurationSource::new(pool)?;
        Self::start_with_source(source, shutdown).await
    }

    /// `InitConfigurationServerFrontendOnly`'s server construction, initial
    /// read and task startup (minus the client mode, passthrough and escape
    /// hatch).
    pub async fn start_with_source(
        source: DbConfigurationSource,
        shutdown: CancellationToken,
    ) -> Result<Self, String> {
        let cached = CachedConfigurationSource::new(
            source,
            CachedConfigurationSource::<DbConfigurationSource>::DEFAULT_TTL,
        );
        let server = Arc::new(Server::new(cached));
        let updates = server
            .take_source_updates()
            .ok_or_else(|| "configuration server already started".to_owned())?;
        let store = Arc::new(ConfigStore::new());
        let live = Arc::new(LiveConfig::new(Unified::default()));

        // The baseline's `Watch` blocks until the store is initialized and
        // then seeds `oldConfig` with the current configuration. Doing the
        // first read before `Server::run` subscribes gives the same seed and
        // avoids a spurious restart from the empty initial snapshot.
        let raw = server
            .source()
            .source()
            .read()
            .await
            .map_err(|err| format!("unable to fetch new configuration: {err}"))?;
        apply_update(&store, &live, &raw)
            .map_err(|err| format!("unable to update new configuration: {err}"))?;

        let mut tasks = JoinSet::new();
        {
            let server = Arc::clone(&server);
            let live = Arc::clone(&live);
            let shutdown = shutdown.clone();
            tasks.spawn(async move { server.run(&live, shutdown).await });
        }
        {
            let server = Arc::clone(&server);
            let store = Arc::clone(&store);
            let live = Arc::clone(&live);
            tasks.spawn(run_update_loop(
                server,
                store,
                live,
                updates,
                shutdown.clone(),
            ));
        }

        Ok(Self {
            server,
            store,
            live,
            shutdown,
            tasks,
        })
    }

    /// The configuration server (write path, restart flag).
    pub fn server(&self) -> &Arc<DbConfigServer> {
        &self.server
    }

    /// The in-memory store (`conf.Raw`/`conf.Get` equivalents).
    pub fn store(&self) -> &Arc<ConfigStore> {
        &self.store
    }

    /// The live snapshot that watchers subscribe to.
    pub fn live(&self) -> &Arc<LiveConfig> {
        &self.live
    }

    /// Cancel the background tasks and wait for them to finish.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        while self.tasks.join_next().await.is_some() {}
    }
}

/// `client.fetchAndUpdate` against the passthrough source: update the store,
/// then replace the live snapshot when the configuration actually changed.
fn apply_update(store: &ConfigStore, live: &LiveConfig, raw: &RawUnified) -> Result<(), String> {
    let (result, outcome) = store.maybe_update(raw);
    outcome.map_err(|err| err.to_string())?;
    if result.changed {
        if let Some(new) = result.new {
            live.set((*new).clone());
        }
    }
    Ok(())
}

/// `client.continuouslyUpdate`'s loop body over the passthrough source.
async fn run_update_loop(
    server: Arc<DbConfigServer>,
    store: Arc<ConfigStore>,
    live: Arc<LiveConfig>,
    mut updates: mpsc::Receiver<oneshot::Sender<()>>,
    shutdown: CancellationToken,
) {
    loop {
        let done = tokio::select! {
            _ = shutdown.cancelled() => return,
            update = updates.recv() => match update {
                Some(done) => Some(done),
                None => return,
            },
            _ = tokio::time::sleep(UPDATE_INTERVAL) => None,
        };

        match server.source().source().read().await {
            Ok(raw) => {
                if let Err(err) = apply_update(&store, &live, &raw) {
                    tracing::error!(error = %err, "unable to update new configuration");
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "unable to fetch new configuration");
            }
        }

        // The baseline always reports completion, even when the read failed,
        // so `Server::write` cannot deadlock on a transient DB error.
        if let Some(done) = done {
            let _ = done.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SITE: &str = r#"{"auth.providers":[{"type":"builtin","allowSignup":true}]}"#;

    fn raw(id: i32, site: &str) -> RawUnified {
        RawUnified {
            id,
            site: site.to_owned(),
            ..RawUnified::default()
        }
    }

    #[test]
    fn apply_update_replaces_the_live_snapshot_only_on_change() {
        let store = ConfigStore::new();
        let live = LiveConfig::new(Unified::default());
        assert_eq!(live.generation(), 0);

        apply_update(&store, &live, &raw(1, SITE)).expect("first update");
        assert_eq!(live.generation(), 1);
        assert_eq!(
            live.current().site.raw()["auth.providers"][0]["allowSignup"],
            true
        );

        // Same raw (the ID is ignored by `RawUnified::Equal`): no new live
        // snapshot and no error.
        apply_update(&store, &live, &raw(2, SITE)).expect("unchanged");
        assert_eq!(live.generation(), 1);

        // Invalid JSON: the raw value advances, the live snapshot does not.
        let err = apply_update(&store, &live, &raw(3, "{")).expect_err("parse");
        assert!(
            err.contains("when parsing rawConfig during update"),
            "{err}"
        );
        assert_eq!(live.generation(), 1);
    }
}
