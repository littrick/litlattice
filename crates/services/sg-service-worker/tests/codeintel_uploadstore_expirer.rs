#![forbid(unsafe_code)]

//! M9.8p differential harness (Rust side): the `codeintel-uploadstore-expirer`
//! job.
//!
//! Mirrors `cmd/worker/internal/codeintel/lsifuploadstore_expirer_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_uploadstore_expirer_diff.sh` points both sides
//! at the live blobstore and the same scratch bucket, then diffs the job
//! description, the loaded config, the routine name and the bucket contents
//! after three `expire_objects` scenarios.

use std::sync::Arc;
use std::time::Duration;

use sg_config::{LiveConfig, Unified};
use sg_core::EnvReader;
use sg_service_worker::jobs::codeintel_uploadstore_expirer::UploadStoreExpirer;
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn upload_store_expirer_matches_go_harness() {
    if std::env::var("SG_EXPIRER_DIFF").ok().as_deref() != Some("1") {
        return;
    }

    let job = UploadStoreExpirer::new();
    job.config().load(&mut EnvReader::new());
    println!("bmeta description={:?}", job.description());
    let config = job.config();
    let store_config = config.upload_store().expect("upload store config");
    println!(
        "ucfg prefix={:?} max_age_seconds={} interval_seconds={} backend={} bucket={} manage_bucket={}",
        config.prefix(),
        config.max_age().as_secs(),
        config.interval().as_secs(),
        store_config.backend,
        store_config.bucket,
        store_config.manage_bucket,
    );

    let deps = JobDeps {
        frontend: PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    for (index, routine) in routines.iter().enumerate() {
        println!("broutine {index} name={}", routine.name());
    }

    let store = sg_codeintel::lsifuploadstore::initialize_store(&store_config)
        .await
        .expect("upload store");
    let keys = [
        "expirer-diff-expire-a",
        "expirer-diff-expire-b",
        "expirer-diff-keep-c",
        "expirer-diff-keep-d",
    ];

    reset(&store, keys).await;
    seed(&store, keys).await;
    store
        .expire_objects("expirer-diff-expire-", Duration::ZERO)
        .await
        .expect("expire prefix max0");
    println!(
        "s1 prefix_max0 keys={}",
        list_keys(&store, "expirer-diff-").await
    );

    seed(&store, keys).await;
    store
        .expire_objects("expirer-diff-expire-", Duration::from_secs(3600))
        .await
        .expect("expire prefix max1h");
    println!(
        "s2 prefix_max1h keys={}",
        list_keys(&store, "expirer-diff-").await
    );

    store
        .expire_objects("expirer-diff-", Duration::ZERO)
        .await
        .expect("expire all max0");
    println!(
        "s3 all_max0 keys={}",
        list_keys(&store, "expirer-diff-").await
    );
}

async fn seed(store: &sg_objectstorage::ObjectStore, keys: [&'static str; 4]) {
    for key in keys {
        store
            .upload(
                key,
                sg_objectstorage::ObjectByteStream::from_static(b"payload"),
            )
            .await
            .expect("seed");
    }
}

async fn reset(store: &sg_objectstorage::ObjectStore, keys: [&'static str; 4]) {
    for key in keys {
        let _ = store.delete(key).await;
    }
}

async fn list_keys(store: &sg_objectstorage::ObjectStore, prefix: &str) -> String {
    let mut keys: Vec<String> = store
        .list_objects(prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|object| object.key)
        .collect();
    keys.sort();
    if keys.is_empty() {
        return "<>".to_owned();
    }
    keys.join(",")
}
