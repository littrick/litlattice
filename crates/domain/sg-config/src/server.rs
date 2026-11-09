#![forbid(unsafe_code)]

//! Port of the frontend configuration server core (`internal/conf/server.go`)
//! and the non-DB configuration sources (`internal/conf/conf.go`).
//!
//! [`Server`] validates a write, forwards it to its [`ConfigurationSource`],
//! and waits until the configuration update loop has read the new value
//! (the `sourceWrites` handshake). [`Server::run`] replaces the baseline's
//! `Start` goroutine: the first observation seeds the comparison, later
//! changes mark the server as needing a restart when
//! [`need_restart_to_apply`] says so.
//!
//! Not ported here (see docs/overviews/m9_8ef_frontend_conf_server):
//! the DB-backed `configurationSource` and fsnotify `watchUpdate`, the
//! generic `Diff` API (it needs the full Go type information), and the
//! startup wiring (`InitConfigurationServerFrontendOnly`). The trait's
//! methods are `async` with native `async fn` futures so the DB source
//! (`sg-service-frontend::configuration_source`, M9.8ei) can await sqlx.

use std::path::PathBuf;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{ConfigError, LiveConfig, RawUnified, SiteConfiguration, Unified};

/// `conf.ConfigurationSource`: direct access to the raw configuration.
///
/// The baseline takes a `context.Context`; the port returns a `Send` future
/// for the same reason. The methods take owned data so the returned futures
/// do not borrow their arguments.
pub trait ConfigurationSource: Send + Sync {
    fn read(&self) -> impl std::future::Future<Output = Result<RawUnified, String>> + Send;
    fn write(
        &self,
        data: RawUnified,
        last_id: i32,
        author_user_id: i32,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// The errors `Server::Write` can return.
#[derive(Debug)]
pub enum ServerError {
    /// `ParseConfig` rejected the input (invalid JSONC).
    Parse(ConfigError),
    /// The configuration source failed.
    Source(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Parse(err) => write!(f, "{err}"),
            ServerError::Source(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ServerError {}

/// `conf.Server`: validates and writes configuration, and tracks whether a
/// pending change requires a restart.
pub struct Server<S: ConfigurationSource> {
    source: S,
    /// The receiver half is handed to the configuration update loop
    /// (`InitConfigurationServerFrontendOnly` sets it as `sourceUpdates`).
    source_writes: mpsc::Sender<oneshot::Sender<()>>,
    source_updates: Mutex<Option<mpsc::Receiver<oneshot::Sender<()>>>>,
    need_restart: RwLock<bool>,
}

impl<S: ConfigurationSource> Server<S> {
    pub fn new(source: S) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Self {
            source,
            source_writes: sender,
            source_updates: Mutex::new(Some(receiver)),
            need_restart: RwLock::new(false),
        }
    }

    /// `Server.Write`: validate, write to the source, then wait until the
    /// update loop reports it has read the new configuration.
    pub async fn write(
        &self,
        input: RawUnified,
        last_id: i32,
        author_user_id: i32,
    ) -> Result<(), ServerError> {
        // `ParseConfig` validates the input before it reaches the source.
        Unified::parse(&input).map_err(ServerError::Parse)?;

        self.source
            .write(input, last_id, author_user_id)
            .await
            .map_err(ServerError::Source)?;

        let (done_sender, done_receiver) = oneshot::channel();
        self.source_writes
            .send(done_sender)
            .await
            .map_err(|_| ServerError::Source("configuration update loop stopped".to_owned()))?;
        done_receiver
            .await
            .map_err(|_| ServerError::Source("configuration update loop stopped".to_owned()))?;
        Ok(())
    }

    /// The receiver for "the source was updated" signals (the baseline's
    /// `server.sourceWrites`, used as `client.sourceUpdates`).
    pub fn take_source_updates(&self) -> Option<mpsc::Receiver<oneshot::Sender<()>>> {
        self.source_updates
            .lock()
            .expect("config source updates lock poisoned")
            .take()
    }

    /// `Server.Start`'s `conf.Watch` goroutine. `Watch` invokes the callback
    /// once immediately (the first observation only seeds `oldConfig`) and
    /// again on every configuration change.
    pub async fn run(&self, live: &LiveConfig, shutdown: CancellationToken) {
        let mut changes = live.subscribe();
        let mut old: Option<std::sync::Arc<Unified>> = Some(live.current());

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = changes.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }

            let new = live.current();
            if let Some(previous) = &old {
                if need_restart_to_apply(previous, &new) {
                    self.mark_need_server_restart();
                }
            }
            old = Some(new);
        }
    }

    /// `Server.NeedServerRestart`.
    pub fn need_server_restart(&self) -> bool {
        *self
            .need_restart
            .read()
            .expect("config restart lock poisoned")
    }

    /// `Server.markNeedServerRestart`.
    pub fn mark_need_server_restart(&self) {
        *self
            .need_restart
            .write()
            .expect("config restart lock poisoned") = true;
    }

    pub fn source(&self) -> &S {
        &self.source
    }
}

/// `conf.needRestartToApply`: whether any changed option is in
/// `requireRestart`.
///
/// The baseline diffs every Go struct field and intersects with
/// `requireRestart`. Only three of its four entries still have typed schema
/// fields; `insights.commit.indexer.interval` has no field in
/// `schema.SiteConfiguration`, so the baseline never reports it (a dead
/// entry) and neither does this port.
///
/// `auth.providers` is compared as raw JSON (unknown fields are not dropped)
/// rather than as the baseline's unmarshaled structs; see the overview §2.3.
pub fn need_restart_to_apply(before: &Unified, after: &Unified) -> bool {
    fn auth_providers(site: &SiteConfiguration) -> Option<&serde_json::Value> {
        match site.raw().get("auth.providers") {
            None | Some(serde_json::Value::Null) => None,
            Some(providers) => Some(providers),
        }
    }

    fn int_option(site: &SiteConfiguration, key: &str) -> i64 {
        site.raw()
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    }

    auth_providers(&before.site) != auth_providers(&after.site)
        || int_option(&before.site, "insights.query.worker.concurrency")
            != int_option(&after.site, "insights.query.worker.concurrency")
        || int_option(&before.site, "permissions.syncUsersMaxConcurrency")
            != int_option(&after.site, "permissions.syncUsersMaxConcurrency")
}

/// `conf.cachedConfigurationSource`: caches `Read` results for a TTL and
/// updates the cache after a successful `Write`.
pub struct CachedConfigurationSource<S: ConfigurationSource> {
    source: S,
    ttl: Duration,
    /// Async because `read`/`write` hold it across awaits.
    entry: tokio::sync::Mutex<Option<(RawUnified, Instant)>>,
}

impl<S: ConfigurationSource> CachedConfigurationSource<S> {
    /// The frontend uses half of `conf.Watch`'s 5s poll interval.
    pub const DEFAULT_TTL: Duration = Duration::from_millis(2500);

    pub fn new(source: S, ttl: Duration) -> Self {
        Self {
            source,
            ttl,
            entry: tokio::sync::Mutex::new(None),
        }
    }

    pub fn source(&self) -> &S {
        &self.source
    }
}

impl<S: ConfigurationSource> ConfigurationSource for CachedConfigurationSource<S> {
    async fn read(&self) -> Result<RawUnified, String> {
        {
            let entry = self.entry.lock().await;
            if let Some((cached, cached_at)) = entry.as_ref() {
                if cached_at.elapsed() <= self.ttl {
                    return Ok(cached.clone());
                }
            }
        }
        let updated = self.source.read().await?;
        *self.entry.lock().await = Some((updated.clone(), Instant::now()));
        Ok(updated)
    }

    async fn write(
        &self,
        data: RawUnified,
        last_id: i32,
        author_user_id: i32,
    ) -> Result<(), String> {
        self.source
            .write(data.clone(), last_id, author_user_id)
            .await?;
        *self.entry.lock().await = Some((data, Instant::now()));
        Ok(())
    }
}

/// `conf.filebasedConfigurationSource`: the `SRC_CONFIGURATION_STUB_FROM_FILE`
/// dev escape hatch. Every read re-reads the file; writes are rejected.
pub struct FileConfigurationSource {
    pub filename: PathBuf,
}

impl FileConfigurationSource {
    pub fn new(filename: impl Into<PathBuf>) -> Self {
        Self {
            filename: filename.into(),
        }
    }
}

impl ConfigurationSource for FileConfigurationSource {
    async fn read(&self) -> Result<RawUnified, String> {
        let content = std::fs::read_to_string(&self.filename).map_err(|err| err.to_string())?;
        let value: serde_json::Value =
            serde_json::from_str(&content).map_err(|err| err.to_string())?;
        let object = value.as_object().ok_or_else(|| {
            "json: cannot unmarshal non-object into Go value of type conftypes.RawUnified"
                .to_owned()
        })?;

        Ok(RawUnified {
            id: field(object, "ID")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0) as i32,
            site: field(object, "Site")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            service_connections: parse_service_connections(field(object, "ServiceConnections")),
        })
    }

    async fn write(
        &self,
        _data: RawUnified,
        _last_id: i32,
        _author_user_id: i32,
    ) -> Result<(), String> {
        Err("tried to write to file based configuration source".to_owned())
    }
}

/// `encoding/json` matches struct fields without tags case-insensitively.
fn field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Option<&'a serde_json::Value> {
    object
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn string_value(value: Option<&serde_json::Value>) -> String {
    value
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn parse_service_connections(value: Option<&serde_json::Value>) -> crate::ServiceConnections {
    let Some(object) = value.and_then(serde_json::Value::as_object) else {
        return crate::ServiceConnections::default();
    };
    crate::ServiceConnections {
        git_servers: string_list(field(object, "GitServers")),
        postgres_dsn: string_value(field(object, "PostgresDSN")),
        code_intel_postgres_dsn: string_value(field(object, "CodeIntelPostgresDSN")),
        code_insights_dsn: string_value(field(object, "CodeInsightsDSN")),
        searchers: string_list(field(object, "Searchers")),
        symbols: string_list(field(object, "Symbols")),
        embeddings: string_list(field(object, "Embeddings")),
        zoekts: string_list(field(object, "Zoekts")),
        zoekt_list_ttl: field(object, "ZoektListTTL")
            .and_then(serde_json::Value::as_i64)
            .map(|nanos| Duration::from_nanos(nanos.max(0) as u64))
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServiceConnections;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct FakeSource {
        raw: Mutex<RawUnified>,
        reads: AtomicUsize,
        writes: AtomicUsize,
        write_error: Option<String>,
    }

    impl FakeSource {
        fn with_site(site: &str) -> Self {
            Self {
                raw: Mutex::new(RawUnified {
                    id: 1,
                    site: site.to_owned(),
                    ..RawUnified::default()
                }),
                ..Self::default()
            }
        }
    }

    impl ConfigurationSource for FakeSource {
        async fn read(&self) -> Result<RawUnified, String> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.raw.lock().expect("fake lock poisoned").clone())
        }

        async fn write(
            &self,
            data: RawUnified,
            _last_id: i32,
            _author_user_id: i32,
        ) -> Result<(), String> {
            if let Some(error) = &self.write_error {
                return Err(error.clone());
            }
            self.writes.fetch_add(1, Ordering::SeqCst);
            *self.raw.lock().expect("fake lock poisoned") = data;
            Ok(())
        }
    }

    fn raw(site: &str) -> RawUnified {
        RawUnified {
            site: site.to_owned(),
            ..RawUnified::default()
        }
    }

    fn parsed(site: &str) -> Unified {
        Unified::parse(&raw(site)).expect("parse")
    }

    #[tokio::test]
    async fn write_waits_for_the_update_loop_after_writing_the_source() {
        let server = std::sync::Arc::new(Server::new(FakeSource::with_site("{}")));
        let mut updates = server.take_source_updates().expect("receiver");
        let input = raw(r#"{"externalURL":"https://example.com"}"#);

        let writer = tokio::spawn({
            let server = std::sync::Arc::clone(&server);
            async move { server.write(input, 1, 7).await }
        });
        // The source is written before the handshake signal is sent.
        let done = updates.recv().await.expect("handshake");
        assert_eq!(server.source().writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            server.source().read().await.expect("read").site,
            r#"{"externalURL":"https://example.com"}"#
        );
        done.send(()).expect("close handshake");
        writer.await.expect("join").expect("write");
    }

    #[tokio::test]
    async fn invalid_json_is_rejected_before_the_source() {
        let server = Server::new(FakeSource::with_site("{}"));
        let error = server.write(raw("{"), 1, 7).await.expect_err("invalid");
        assert!(matches!(error, ServerError::Parse(_)));
        assert_eq!(server.source().writes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_write_errors_are_propagated() {
        let server = Server::new(FakeSource {
            write_error: Some("boom".to_owned()),
            ..FakeSource::with_site("{}")
        });
        let error = server
            .write(raw("{}"), 1, 7)
            .await
            .expect_err("source error");
        assert!(matches!(error, ServerError::Source(message) if message == "boom"));
    }

    #[test]
    fn restart_requires_one_of_the_live_restart_options() {
        assert!(!need_restart_to_apply(&parsed("{}"), &parsed("{}")));
        assert!(!need_restart_to_apply(
            &parsed(r#"{"externalURL":"a"}"#),
            &parsed(r#"{"externalURL":"b"}"#)
        ));
        assert!(need_restart_to_apply(
            &parsed("{}"),
            &parsed(r#"{"auth.providers":[{"type":"builtin"}]}"#)
        ));
        assert!(need_restart_to_apply(
            &parsed("{}"),
            &parsed(r#"{"insights.query.worker.concurrency":5}"#)
        ));
        assert!(!need_restart_to_apply(
            &parsed("{}"),
            &parsed(r#"{"insights.query.worker.concurrency":0}"#)
        ));
        assert!(need_restart_to_apply(
            &parsed("{}"),
            &parsed(r#"{"permissions.syncUsersMaxConcurrency":8}"#)
        ));
        assert!(!need_restart_to_apply(
            &parsed(r#"{"insights.commit.indexer.interval":"1m"}"#),
            &parsed(r#"{"insights.commit.indexer.interval":"2m"}"#)
        ));
        // `experimentalFeatures` changes are not restart options.
        assert!(!need_restart_to_apply(
            &parsed(r#"{"experimentalFeatures":{"structuralSearch":"enabled"}}"#),
            &parsed(r#"{"experimentalFeatures":{"structuralSearch":"disabled"}}"#)
        ));
        // Service connection changes are not restart options either.
        let mut before = parsed("{}");
        before.service_connections = ServiceConnections {
            git_servers: vec!["g1".to_owned()],
            ..ServiceConnections::default()
        };
        let mut after = parsed("{}");
        after.service_connections = ServiceConnections {
            git_servers: vec!["g2".to_owned()],
            ..ServiceConnections::default()
        };
        assert!(!need_restart_to_apply(&before, &after));
    }

    #[tokio::test]
    async fn run_ignores_plain_changes_and_marks_restart_options() {
        let live = std::sync::Arc::new(LiveConfig::new(parsed("{}")));
        let server = std::sync::Arc::new(Server::new(FakeSource::with_site("{}")));
        let shutdown = CancellationToken::new();
        let runner = tokio::spawn({
            let server = std::sync::Arc::clone(&server);
            let live = std::sync::Arc::clone(&live);
            let shutdown = shutdown.clone();
            async move { server.run(&live, shutdown).await }
        });

        // A plain field change does not require a restart.
        live.set(parsed(r#"{"externalURL":"https://example.com"}"#));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!server.need_server_restart());

        // A restart option does.
        live.set(parsed(r#"{"auth.providers":[{"type":"builtin"}]}"#));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(server.need_server_restart());

        shutdown.cancel();
        runner.await.expect("join");
    }

    #[tokio::test]
    async fn cached_source_reuses_the_entry_within_the_ttl() {
        let cached =
            CachedConfigurationSource::new(FakeSource::with_site("{}"), Duration::from_secs(60));
        assert_eq!(cached.read().await.expect("read").site, "{}");
        assert_eq!(cached.read().await.expect("read").site, "{}");
        assert_eq!(cached.source.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cached_source_rereads_after_the_ttl() {
        let cached = CachedConfigurationSource::new(FakeSource::with_site("{}"), Duration::ZERO);
        cached.read().await.expect("read");
        cached.read().await.expect("read");
        assert_eq!(cached.source.reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cached_source_updates_the_entry_after_a_write() {
        let cached =
            CachedConfigurationSource::new(FakeSource::with_site("{}"), Duration::from_secs(60));
        cached.read().await.expect("read");
        cached
            .write(raw(r#"{"externalURL":"https://example.com"}"#), 1, 7)
            .await
            .expect("write");
        assert_eq!(
            cached.read().await.expect("read").site,
            r#"{"externalURL":"https://example.com"}"#
        );
        assert_eq!(cached.source.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn file_source_reads_the_raw_unified_shape_and_rejects_writes() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "sg-config-file-source-{}-{unique}.json",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"{"ID":5,"Site":"{}","ServiceConnections":{"GitServers":["g9"],"PostgresDSN":"pg"}}"#,
        )
        .expect("write fixture");
        let source = FileConfigurationSource::new(path.clone());
        let raw = source.read().await.expect("read");
        assert_eq!(raw.id, 5);
        assert_eq!(raw.site, "{}");
        assert_eq!(raw.service_connections.git_servers, ["g9"]);
        assert_eq!(raw.service_connections.postgres_dsn, "pg");
        assert_eq!(
            source.write(raw.clone(), 1, 7).await.expect_err("write"),
            "tried to write to file based configuration source"
        );
        let _ = std::fs::remove_file(path);
    }
}
