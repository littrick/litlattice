#![forbid(unsafe_code)]

//! M9.8ak differential harness (Rust side): the `codehost-version-syncing`
//! job.
//!
//! Mirrors `internal/extsvc/versions/harness_sync_test.go`.
//! `test_ws/p9-go/run_codehost_version_syncing_diff.sh` applies the shared
//! fixtures to a scratch frontend database immediately before each side and
//! diffs the canonical lines: job/routine metadata, the versions loaded from
//! the fixture services, the JSON written to Redis and the no-services,
//! unknown-kind and source-error cases.
//!
//! The fake sourcer matches the Go harness: GitHub, GitLab and Bitbucket
//! Server sources return `version-<id>`, `extsvc:github:9706` fails the fetch,
//! and every other kind is not a VersionSource.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, Unified};
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_keyring::Keyring;
use sg_redis::KeyValue;
use sg_service_worker::jobs::codehost_version_syncing::{
    encode_versions, load_versions, CodehostVersionSyncing, ExternalServiceLister, Source, Sourcer,
    VersionSource, VERSIONS_KEY,
};
use sg_service_worker::jobs::{Job, JobDeps};
use tokio_util::sync::CancellationToken;

const PREFIX: &str = "m98ak";

struct FakeSourcer {
    fail_urn: Option<String>,
}

struct FixedVersion(String);

#[async_trait]
impl VersionSource for FixedVersion {
    async fn version(&self) -> Result<String, String> {
        Ok(self.0.clone())
    }
}

struct FailingVersion;

#[async_trait]
impl VersionSource for FailingVersion {
    async fn version(&self) -> Result<String, String> {
        Err("fetch failed".to_owned())
    }
}

#[async_trait]
impl Sourcer for FakeSourcer {
    async fn source(&self, svc: &ExternalService) -> Result<Source, String> {
        let urn = sg_extsvc::urn(&svc.kind, svc.id);
        if self.fail_urn.as_deref() == Some(urn.as_str()) {
            return Err("create source: boom".to_owned());
        }
        if urn == "extsvc:github:9706" {
            return Ok(Source::Version(Arc::new(FailingVersion)));
        }
        match svc.kind.to_uppercase().as_str() {
            "GITHUB" | "GITLAB" | "BITBUCKETSERVER" => Ok(Source::Version(Arc::new(FixedVersion(
                format!("version-{}", svc.id),
            )))),
            _ => Ok(Source::Other),
        }
    }
}

struct FakeLister(Vec<ExternalService>);

#[async_trait]
impl ExternalServiceLister for FakeLister {
    async fn list(&self) -> Result<Vec<ExternalService>, String> {
        Ok(self.0.clone())
    }
}

fn print_versions(versions: &[sg_service_worker::jobs::codehost_version_syncing::Version]) {
    let mut sorted: Vec<_> = versions.to_vec();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    for version in &sorted {
        println!(
            "cv version kind={} key={} version={}",
            version.external_service_kind, version.key, version.version
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn codehost_version_syncing_matches_go_harness() {
    if std::env::var("SG_CV_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let dsn = std::env::var("SG_CV_DSN").expect("SG_CV_DSN");
    let endpoint =
        std::env::var("SG_CV_REDIS_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    let job = CodehostVersionSyncing::new();
    println!("cv job description={:?}", job.description());
    println!("cv job configs={}", job.configs().len());

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified {
            service_connections: ServiceConnections::default(),
            ..Default::default()
        })),
        keyring: Arc::new(Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    let routine = &routines[0];
    println!("cv routine name={}", routine.name());
    println!("cv routine description={:?}", routine.description());
    println!("cv routine interval_ms={}", routine.interval().as_millis());

    let kv = KeyValue::connect(&endpoint)
        .await
        .expect("connect redis")
        .with_prefix(PREFIX);
    kv.del(VERSIONS_KEY).await.expect("clear versions key");

    let store = ExternalServiceStore::new(pool.clone());

    // Scenario A: the fixture services.
    let mut versions = load_versions(&store, &deps.keyring, &FakeSourcer { fail_urn: None })
        .await
        .expect("load versions");
    print_versions(&versions);

    versions.sort_by(|a, b| a.key.cmp(&b.key));
    let payload = encode_versions(&versions);
    kv.set(VERSIONS_KEY, &payload).await.expect("set versions");
    println!("cv payload={payload}");
    println!(
        "cv stored={}",
        kv.get_string(VERSIONS_KEY)
            .await
            .expect("get versions")
            .unwrap_or_default()
    );

    // Scenario B: no external services -> `json.Marshal(nil)` is `null`.
    let empty = load_versions(
        &FakeLister(Vec::new()),
        &deps.keyring,
        &FakeSourcer { fail_urn: None },
    )
    .await
    .expect("load empty versions");
    let empty_payload = encode_versions(&empty);
    kv.set(VERSIONS_KEY, &empty_payload)
        .await
        .expect("set empty versions");
    println!("cv empty payload={empty_payload}");
    println!(
        "cv empty stored={}",
        kv.get_string(VERSIONS_KEY)
            .await
            .expect("get empty versions")
            .unwrap_or_default()
    );

    // Scenario C: an unknown kind fails before its config is parsed.
    let bad = ExternalService {
        id: 9801,
        kind: "FOO".to_owned(),
        display_name: "bad".to_owned(),
        config: "not json".to_owned(),
        encryption_key_id: String::new(),
        last_sync_at: None,
        next_sync_at: None,
        unrestricted: false,
        has_webhooks: None,
        code_host_id: None,
    };
    let err = load_versions(
        &FakeLister(vec![bad]),
        &deps.keyring,
        &FakeSourcer { fail_urn: None },
    )
    .await
    .expect_err("unknown kind must fail");
    println!("cv badkind err={err:?}");

    // Scenario D: a source-construction failure aborts the tick.
    let err = load_versions(
        &store,
        &deps.keyring,
        &FakeSourcer {
            fail_urn: Some("extsvc:gitlab:9703".to_owned()),
        },
    )
    .await
    .expect_err("source error must fail");
    println!("cv sourcerr err={err:?}");
}
