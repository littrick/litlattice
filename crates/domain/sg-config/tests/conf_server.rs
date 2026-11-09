//! M9.8ef differential harness: drives `sg_config::server` through the same
//! cases as the Go `internal/conf` harness and prints the identical format.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sg_config::server::{
    need_restart_to_apply, CachedConfigurationSource, ConfigurationSource, FileConfigurationSource,
    Server, ServerError,
};
use sg_config::{RawUnified, Unified};

#[derive(Clone, Default)]
struct Counters {
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
}

struct FakeSource {
    raw: Mutex<RawUnified>,
    counters: Counters,
    write_error: Option<String>,
}

impl FakeSource {
    fn with_site(site: &str, counters: Counters) -> Self {
        Self {
            raw: Mutex::new(RawUnified {
                id: 1,
                site: site.to_owned(),
                ..RawUnified::default()
            }),
            counters,
            write_error: None,
        }
    }

    fn site(&self) -> String {
        self.raw.lock().expect("fake lock poisoned").site.clone()
    }
}

impl ConfigurationSource for FakeSource {
    async fn read(&self) -> Result<RawUnified, String> {
        self.counters.reads.fetch_add(1, Ordering::SeqCst);
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
        self.counters.writes.fetch_add(1, Ordering::SeqCst);
        *self.raw.lock().expect("fake lock poisoned") = data;
        Ok(())
    }
}

fn parsed(site: &str) -> Unified {
    Unified::parse(&RawUnified {
        site: site.to_owned(),
        ..RawUnified::default()
    })
    .expect("parse")
}

fn restart_harness() {
    let cases: &[(&str, &str, &str)] = &[
        ("no-change", "{}", "{}"),
        (
            "external-url",
            r#"{"externalURL":"a"}"#,
            r#"{"externalURL":"b"}"#,
        ),
        (
            "auth-add",
            "{}",
            r#"{"auth.providers":[{"type":"builtin"}]}"#,
        ),
        (
            "auth-reorder",
            r#"{"auth.providers":[{"type":"builtin"},{"type":"github","clientID":"x"}]}"#,
            r#"{"auth.providers":[{"type":"github","clientID":"x"},{"type":"builtin"}]}"#,
        ),
        (
            "auth-same",
            r#"{"auth.providers":[{"type":"builtin"}]}"#,
            r#"{"auth.providers":[{"type":"builtin"}]}"#,
        ),
        (
            "concurrency-change",
            r#"{"insights.query.worker.concurrency":5}"#,
            r#"{"insights.query.worker.concurrency":0}"#,
        ),
        (
            "concurrency-absent-zero",
            "{}",
            r#"{"insights.query.worker.concurrency":0}"#,
        ),
        (
            "permissions-change",
            "{}",
            r#"{"permissions.syncUsersMaxConcurrency":8}"#,
        ),
        (
            "experimental-change",
            r#"{"experimentalFeatures":{"structuralSearch":"enabled"}}"#,
            r#"{"experimentalFeatures":{"structuralSearch":"disabled"}}"#,
        ),
        (
            "dead-key",
            r#"{"insights.commit.indexer.interval":"1m"}"#,
            r#"{"insights.commit.indexer.interval":"2m"}"#,
        ),
    ];
    for (name, before, after) in cases {
        println!(
            "nr {name} restart={}",
            need_restart_to_apply(&parsed(before), &parsed(after))
        );
    }

    let mut before = parsed("{}");
    before.service_connections.git_servers = vec!["g1".to_owned()];
    let mut after = parsed("{}");
    after.service_connections.git_servers = vec!["g2".to_owned()];
    println!(
        "nr service-connections restart={}",
        need_restart_to_apply(&before, &after)
    );
}

fn class(error: &ServerError) -> &'static str {
    match error {
        ServerError::Parse(_) => "parse",
        ServerError::Source(_) => "source",
    }
}

async fn write_harness() {
    // Valid write: the source is written before the handshake signal, and
    // Write returns only after the reader closes the handshake.
    let counters = Counters::default();
    let server = Arc::new(Server::new(FakeSource::with_site("{}", counters.clone())));
    let mut updates = server.take_source_updates().expect("receiver");
    let input = RawUnified {
        id: 1,
        site: r#"{"externalURL":"https://example.com"}"#.to_owned(),
        ..RawUnified::default()
    };
    let writer = tokio::spawn({
        let server = Arc::clone(&server);
        async move { server.write(input, 1, 7).await }
    });
    let done = updates.recv().await.expect("handshake");
    let writes = counters.writes.load(Ordering::SeqCst);
    let site = server.source().site();
    let marker = if writes == 1 && site == r#"{"externalURL":"https://example.com"}"# {
        "after-write"
    } else {
        "mismatch"
    };
    done.send(()).expect("close handshake");
    writer.await.expect("join").expect("write");
    println!("sv write-valid err=none writes={writes} site={site:?} done={marker}");

    // Invalid JSON is rejected before the source is touched.
    let counters = Counters::default();
    let server = Server::new(FakeSource::with_site("{}", counters.clone()));
    let error = server
        .write(
            RawUnified {
                site: "{".to_owned(),
                ..RawUnified::default()
            },
            1,
            7,
        )
        .await
        .expect_err("parse error");
    println!(
        "sv write-invalid err={} writes={} site={:?} done=-",
        class(&error),
        counters.writes.load(Ordering::SeqCst),
        server.source().site()
    );

    // Source errors are propagated.
    let counters = Counters::default();
    let mut source = FakeSource::with_site("{}", counters.clone());
    source.write_error = Some("boom".to_owned());
    let server = Server::new(source);
    let error = server
        .write(
            RawUnified {
                site: "{}".to_owned(),
                ..RawUnified::default()
            },
            1,
            7,
        )
        .await
        .expect_err("source error");
    println!(
        "sv write-source-error err={} writes={} site={:?} done=-",
        class(&error),
        counters.writes.load(Ordering::SeqCst),
        server.source().site()
    );
}

async fn cached_source_harness() {
    let counters = Counters::default();
    let cached = CachedConfigurationSource::new(
        FakeSource::with_site("{}", counters.clone()),
        Duration::from_secs(3600),
    );
    cached.read().await.expect("read");
    let raw = cached.read().await.expect("read");
    println!(
        "cg ttl-hit reads={} site={:?}",
        counters.reads.load(Ordering::SeqCst),
        raw.site
    );

    let counters = Counters::default();
    let cached = CachedConfigurationSource::new(
        FakeSource::with_site("{}", counters.clone()),
        Duration::ZERO,
    );
    cached.read().await.expect("read");
    std::thread::sleep(Duration::from_millis(1));
    cached.read().await.expect("read");
    println!(
        "cg ttl-zero reads={} site={:?}",
        counters.reads.load(Ordering::SeqCst),
        cached.source().site()
    );

    let counters = Counters::default();
    let cached = CachedConfigurationSource::new(
        FakeSource::with_site("{}", counters.clone()),
        Duration::from_secs(3600),
    );
    cached.read().await.expect("read");
    cached
        .write(
            RawUnified {
                site: r#"{"externalURL":"https://example.com"}"#.to_owned(),
                ..RawUnified::default()
            },
            1,
            7,
        )
        .await
        .expect("write");
    let raw = cached.read().await.expect("read");
    println!(
        "cg write-updates reads={} site={:?}",
        counters.reads.load(Ordering::SeqCst),
        raw.site
    );

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!(
        "sg-config-server-harness-{}-{unique}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"{"ID":5,"Site":"{}","ServiceConnections":{"GitServers":["g9"],"PostgresDSN":"pg"}}"#,
    )
    .expect("write fixture");
    let file = FileConfigurationSource::new(path.clone());
    let raw = file.read().await.expect("read");
    println!(
        "cg file-read id={} site={:?} git={} pg={}",
        raw.id,
        raw.site,
        raw.service_connections.git_servers.join(","),
        raw.service_connections.postgres_dsn
    );
    assert!(file
        .write(
            RawUnified {
                site: "{}".to_owned(),
                ..RawUnified::default()
            },
            1,
            7,
        )
        .await
        .is_err());
    println!("cg file-write err=source");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn conf_server_harness() {
    // Fixed order mirrors the Go harness file order.
    restart_harness();
    write_harness().await;
    cached_source_harness().await;
}
