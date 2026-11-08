#![forbid(unsafe_code)]

//! Differential-style integration tests for `repos.Syncer` against a real
//! Postgres database: the same store SQL the Go baseline uses, driven by the
//! Rust syncer.
//!
//! Gated on `SG_TEST_DATABASE_DSN` (the isolated `sourcegraph_rust_test`
//! database); the dev database has a live repo-updater that would race the
//! queue and timestamp assertions.

use std::sync::Arc;
use std::time::Duration;

use sg_db::external_service_repos::ExternalServiceReposStore;
use sg_db::external_services::ExternalServiceStore;
use sg_service_repo_updater::metrics::Metrics;
use sg_service_repo_updater::scheduler::SystemClock;
use sg_service_repo_updater::syncer::{
    ExtsvcSourceFactory, NoopProgressRecorder, StaticLicenseKey, SyncProgress, Syncer,
};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("sync-{nanos}")
}

async fn test_pool() -> Option<PgPool> {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return None;
    };
    Some(sg_db::connect(&dsn, 5).await.expect("connect"))
}

async fn insert_service(pool: &PgPool, config: &str) -> i64 {
    insert_service_with_unrestricted(pool, config, true).await
}

/// `unrestricted = true` makes the `OTHER` source yield public repositories;
/// `false` yields private ones (`internal/repos/other.go`).
async fn insert_service_with_unrestricted(pool: &PgPool, config: &str, unrestricted: bool) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO external_services(kind, display_name, config, unrestricted) \
         VALUES ('OTHER', 'other-sync-test', $1, $2) RETURNING id",
    )
    .bind(config)
    .bind(unrestricted)
    .fetch_one(pool)
    .await
    .expect("insert external service")
}

async fn cleanup(pool: &PgPool, service_id: i64, suffix: &str) {
    sqlx::query("DELETE FROM external_service_repos WHERE external_service_id = $1")
        .bind(service_id)
        .execute(pool)
        .await
        .expect("cleanup associations");
    sqlx::query("DELETE FROM repo WHERE name LIKE $1")
        .bind(format!("%{suffix}.example.com/%"))
        .execute(pool)
        .await
        .expect("cleanup repos");
    sqlx::query("DELETE FROM external_services WHERE id = $1")
        .bind(service_id)
        .execute(pool)
        .await
        .expect("cleanup service");
}

fn syncer(
    pool: &PgPool,
) -> (
    Syncer,
    tokio::sync::broadcast::Receiver<sg_extsvc::RepoSyncDiff>,
) {
    syncer_with(pool, Arc::new(sg_keyring::Keyring::default()))
}

fn syncer_with(
    pool: &PgPool,
    keyring: Arc<sg_keyring::Keyring>,
) -> (
    Syncer,
    tokio::sync::broadcast::Receiver<sg_extsvc::RepoSyncDiff>,
) {
    let (tx, rx) = tokio::sync::broadcast::channel(64);
    let syncer = Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()),
        // `OTHER` sources configured with explicit clone URLs never touch
        // the HTTP client.
        Arc::new(ExtsvcSourceFactory::new(None, keyring)),
        Some(tx),
        Arc::new(SystemClock),
        Arc::new(Metrics::new()),
        // No license key: the free plan's private-repository limit applies.
        Arc::new(StaticLicenseKey::default()),
    );
    (syncer, rx)
}

async fn repo_names(pool: &PgPool, suffix: &str) -> Vec<(String, bool)> {
    let rows = sqlx::query_as::<_, (String, bool)>(
        "SELECT name, deleted_at IS NOT NULL FROM repo \
         WHERE name LIKE $1 ORDER BY name",
    )
    .bind(format!("%{suffix}.example.com/%"))
    .fetch_all(pool)
    .await
    .expect("list repos");
    rows
}

#[tokio::test]
async fn syncs_creates_updates_and_only_deletes_on_success() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id = insert_service(
        &pool,
        &format!(r#"{{"url":"{base}","repos":["/r1.git","/r2.git"]}}"#),
    )
    .await;

    let (syncer, mut diffs) = syncer(&pool);
    let token = CancellationToken::new();
    let min = Duration::from_secs(60);

    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("first sync");

    let names = repo_names(&pool, &suffix).await;
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(names.iter().all(|(_, deleted)| !deleted));

    // One diff per created repo, each with the database id set.
    let mut added = 0;
    while let Ok(diff) = diffs.try_recv() {
        assert_eq!(diff.added.len(), 1);
        assert!(diff.added[0].id > 0);
        added += diff.added.len();
    }
    assert_eq!(added, 2);

    // The association carries the clone URL.
    let clone_urls: Vec<String> = sqlx::query_scalar(
        "SELECT clone_url FROM external_service_repos WHERE external_service_id = $1 \
         ORDER BY clone_url",
    )
    .bind(service_id)
    .fetch_all(&pool)
    .await
    .expect("associations");
    assert_eq!(
        clone_urls,
        vec![format!("{base}/r1.git"), format!("{base}/r2.git")]
    );

    // The timestamps are always written, and a first sync backs off by the
    // minimum interval.
    let (last, next): (Option<sg_db::OffsetDateTime>, Option<sg_db::OffsetDateTime>) =
        sqlx::query_as("SELECT last_sync_at, next_sync_at FROM external_services WHERE id = $1")
            .bind(service_id)
            .fetch_one(&pool)
            .await
            .expect("timestamps");
    let (last, next) = (last.expect("last_sync_at"), next.expect("next_sync_at"));
    assert_eq!(next - last, time::Duration::seconds(60));

    // A second sync with no source changes yields unmodified repos and no
    // writes.
    // `createRepoQuery` does not set `updated_at`; an unmodified sync must
    // leave it NULL.
    let updated_before: Vec<Option<sg_db::OffsetDateTime>> =
        sqlx::query_scalar("SELECT updated_at FROM repo WHERE name LIKE $1 ORDER BY name")
            .bind(format!("%{suffix}.example.com/%"))
            .fetch_all(&pool)
            .await
            .expect("updated_at");
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("second sync");
    let mut unmodified = 0;
    while let Ok(diff) = diffs.try_recv() {
        unmodified += diff.unmodified.len();
    }
    assert_eq!(unmodified, 2);
    let updated_after: Vec<Option<sg_db::OffsetDateTime>> =
        sqlx::query_scalar("SELECT updated_at FROM repo WHERE name LIKE $1 ORDER BY name")
            .bind(format!("%{suffix}.example.com/%"))
            .fetch_all(&pool)
            .await
            .expect("updated_at");
    assert_eq!(updated_before, updated_after);

    // Changing the sourced set soft-deletes the repo that disappeared and
    // creates the new one.
    sqlx::query("UPDATE external_services SET config = $2 WHERE id = $1")
        .bind(service_id)
        .bind(format!(
            r#"{{"url":"{base}","repos":["/r1.git","/r3.git"]}}"#
        ))
        .execute(&pool)
        .await
        .expect("update config");
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("third sync");

    let names = repo_names(&pool, &suffix).await;
    let live: Vec<&(String, bool)> = names.iter().filter(|(_, deleted)| !deleted).collect();
    assert_eq!(live.len(), 2, "{names:?}");
    let deleted = names
        .iter()
        .find(|(_, deleted)| *deleted)
        .expect("r2 soft-deleted");
    assert!(deleted.0.starts_with("DELETED-"), "{deleted:?}");
    assert!(deleted.0.ends_with(&format!("{suffix}.example.com/r2")));

    let mut deleted_ids = Vec::new();
    while let Ok(diff) = diffs.try_recv() {
        deleted_ids.extend(diff.deleted.iter().map(|repo| repo.id));
    }
    assert_eq!(deleted_ids.len(), 1);

    // Now make the source fail: the previous repos must survive.
    sqlx::query("UPDATE external_services SET config = $2 WHERE id = $1")
        .bind(service_id)
        .bind(format!(r#"{{"url":"{base}","repos":["src-expose"]}}"#))
        .execute(&pool)
        .await
        .expect("update config");
    let err = syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect_err("source failure");
    assert!(
        err.contains("src-expose HTTP client is not configured"),
        "{err}"
    );
    let live_after = repo_names(&pool, &suffix)
        .await
        .into_iter()
        .filter(|(_, deleted)| !deleted)
        .count();
    assert_eq!(live_after, 2, "an errored sync must not delete repos");

    cleanup(&pool, service_id, &suffix).await;
}

#[tokio::test]
async fn revives_soft_deleted_repos_and_reports_progress() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id =
        insert_service(&pool, &format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#)).await;

    let (syncer, _diffs) = syncer(&pool);
    let token = CancellationToken::new();
    let progress = Arc::new(std::sync::Mutex::new(Vec::<SyncProgress>::new()));

    struct Recording(Arc<std::sync::Mutex<Vec<SyncProgress>>>);
    #[async_trait::async_trait]
    impl sg_service_repo_updater::syncer::ProgressRecorder for Recording {
        async fn record(&self, progress: SyncProgress, final_call: bool) -> Result<(), String> {
            if final_call {
                self.0.lock().expect("progress").push(progress);
            }
            Ok(())
        }
    }

    syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &Recording(Arc::clone(&progress)),
            &token,
        )
        .await
        .expect("first sync");

    let recorded = progress.lock().expect("progress").clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].synced, 1);
    assert_eq!(recorded[0].added, 1);
    assert_eq!(recorded[0].errors, 0);

    // Soft-delete the repo out of band, then sync again: the source still
    // yields it, so the syncer clears `deleted_at` and reports it as added.
    sqlx::query("UPDATE repo SET deleted_at = NOW() WHERE name LIKE $1")
        .bind(format!("%{suffix}.example.com/%"))
        .execute(&pool)
        .await
        .expect("soft delete");
    syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &Recording(Arc::clone(&progress)),
            &token,
        )
        .await
        .expect("second sync");

    let recorded = progress.lock().expect("progress").clone();
    assert_eq!(recorded[1].added, 1, "{:?}", recorded[1]);

    let live = repo_names(&pool, &suffix)
        .await
        .into_iter()
        .filter(|(_, deleted)| !deleted)
        .count();
    assert_eq!(live, 1);

    cleanup(&pool, service_id, &suffix).await;
}

/// `Syncer.observeSync` / `ObservedSource.ListRepos` / `observeDiff` must
/// publish the same families the baseline does.
#[tokio::test]
async fn emits_syncer_and_source_metrics() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id =
        insert_service(&pool, &format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#)).await;

    let metrics = Arc::new(Metrics::new());
    let syncer = Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()),
        Arc::new(ExtsvcSourceFactory::new(
            None,
            Arc::new(sg_keyring::Keyring::default()),
        )),
        None,
        Arc::new(SystemClock),
        Arc::clone(&metrics),
        Arc::new(StaticLicenseKey::default()),
    );
    syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &NoopProgressRecorder,
            &CancellationToken::new(),
        )
        .await
        .expect("sync");

    let family = sg_service_repo_updater::metrics::SYNC_FAMILY;
    assert_eq!(
        metrics
            .syncer_start_sync
            .with_label_values(&[family, "site"])
            .get(),
        1.0
    );
    assert!(
        metrics
            .syncer_sync_last_time
            .with_label_values(&[family])
            .get()
            > 0.0
    );
    assert_eq!(
        metrics
            .syncer_sync_duration_seconds
            .with_label_values(&["true", family])
            .get_sample_count(),
        1
    );
    assert_eq!(
        metrics
            .syncer_sync_errors_total
            .with_label_values(&[family, "site", "unknown"])
            .get(),
        0.0
    );
    assert_eq!(
        metrics
            .syncer_synced_repos_total
            .with_label_values(&["added"])
            .get(),
        1.0
    );
    assert_eq!(metrics.source_repos_total.with_label_values(&[]).get(), 1.0);
    assert_eq!(
        metrics
            .source_duration_seconds
            .with_label_values(&[])
            .get_sample_count(),
        1
    );
    assert_eq!(
        metrics.source_errors_total.with_label_values(&[]).get(),
        0.0
    );

    cleanup(&pool, service_id, &suffix).await;
}

/// `NewStoreMetrics`: the sync transaction (`Transact`/`Done`), the
/// create/update/delete writes, and the nested `Transact`/`Done` pair the
/// baseline opens per repo inside `DeleteExternalServiceRepo`.
#[tokio::test]
async fn emits_store_metrics_for_create_update_and_delete() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id =
        insert_service(&pool, &format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#)).await;

    let metrics = Arc::new(Metrics::new());
    let syncer = Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()).with_observer(
            Arc::clone(&metrics) as Arc<dyn sg_db::external_service_repos::StoreObserver>
        ),
        Arc::new(ExtsvcSourceFactory::new(
            None,
            Arc::new(sg_keyring::Keyring::default()),
        )),
        None,
        Arc::new(SystemClock),
        Arc::clone(&metrics),
        Arc::new(StaticLicenseKey::default()),
    );
    let token = CancellationToken::new();
    let min = Duration::from_secs(60);

    // Sync 1: create r1 → one transaction + one create.
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("create sync");

    // Sync 2: make the stored row differ so the update path runs.
    sqlx::query("UPDATE repo SET description = 'stale' WHERE name LIKE $1")
        .bind(format!("%{suffix}.example.com/%"))
        .execute(&pool)
        .await
        .expect("stale description");
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("update sync");

    // Sync 3: create r2 and delete r1.
    sqlx::query("UPDATE external_services SET config = $2 WHERE id = $1")
        .bind(service_id)
        .bind(format!(r#"{{"url":"{base}","repos":["/r2.git"]}}"#))
        .execute(&pool)
        .await
        .expect("update config");
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("delete sync");

    assert_eq!(
        metrics.store_transact_total.with_label_values(&[]).get(),
        4.0,
        "three sync transactions plus the per-delete one"
    );
    assert_eq!(metrics.store_done_total.with_label_values(&[]).get(), 4.0);
    assert_eq!(
        metrics
            .store_transact_errors_total
            .with_label_values(&[])
            .get(),
        0.0
    );
    assert_eq!(
        metrics.store_done_errors_total.with_label_values(&[]).get(),
        0.0
    );
    assert_eq!(
        metrics
            .store_transact_duration_seconds
            .with_label_values(&[])
            .get_sample_count(),
        4
    );
    assert_eq!(
        metrics
            .store_create_external_service_repo_total
            .with_label_values(&[])
            .get(),
        2.0
    );
    assert_eq!(
        metrics
            .store_update_external_service_repo_total
            .with_label_values(&[])
            .get(),
        1.0
    );
    assert_eq!(
        metrics
            .store_delete_external_service_repo_total
            .with_label_values(&[])
            .get(),
        1.0
    );
    assert_eq!(
        metrics
            .store_delete_external_service_repos_not_in_total
            .with_label_values(&[])
            .get(),
        3.0,
        "the delete phase runs on every successful sync"
    );
    for family in [
        &metrics.store_create_external_service_repo_errors_total,
        &metrics.store_update_external_service_repo_errors_total,
        &metrics.store_delete_external_service_repo_errors_total,
        &metrics.store_delete_external_service_repos_not_in_errors_total,
    ] {
        assert_eq!(family.with_label_values(&[]).get(), 0.0);
    }

    cleanup(&pool, service_id, &suffix).await;
}

/// `CreateRepoLicenseHook` with no license key configured: the implicit free
/// plan (`free-1`) allows a single private repository, and the baseline's
/// `LicenseError` does not abort the delete phase.
#[tokio::test]
async fn license_hook_rejects_private_repos_beyond_the_free_plan() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    // Occupy the free plan's single private-repository slot so the sourced
    // private repo is rejected regardless of what else is in the database.
    sqlx::query("INSERT INTO repo(name, private) VALUES ($1, true)")
        .bind(format!("placeholder-{suffix}.example.com/private"))
        .execute(&pool)
        .await
        .expect("placeholder private repo");

    // `unrestricted = false`: the `OTHER` source marks its repos private
    // (`internal/repos/other.go`).
    let service_id = insert_service_with_unrestricted(
        &pool,
        &format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#),
        false,
    )
    .await;

    let (syncer, _diffs) = syncer(&pool);
    let err = syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &NoopProgressRecorder,
            &CancellationToken::new(),
        )
        .await
        .expect_err("private repo must be rejected");
    assert_eq!(
        err,
        format!(
            "syncer: failed to update repo {suffix}.example.com/r1: \
             maximum number of private repositories included in license (1) reached"
        )
    );

    // The repo was never created. The delete phase still ran (a license error
    // is the one error kind that does not abort it), but the service owns no
    // previous repos, so nothing was deleted.
    assert!(!repo_names(&pool, &suffix)
        .await
        .iter()
        .any(|(name, _)| name.ends_with("/r1")));

    cleanup(&pool, service_id, &suffix).await;
}

/// `svc.Config.Decrypt(ctx)` with `keyring.Default().ExternalServiceKey`: an
/// encrypted configuration is decrypted before the source parses it, and a
/// missing key is reported with the baseline's `key mismatch` message.
#[tokio::test]
async fn decrypts_encrypted_external_service_configs() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");

    // `encryption.keys.externalServiceKey = {type: mounted, filepath}`. The
    // Go-compatible envelope format is covered by `sg-encryption`'s fixtures.
    let dir = std::env::temp_dir().join(format!("sg-keyring-{suffix}"));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("external-service-key");
    std::fs::write(&path, [7u8; 32]).expect("write secret");
    let site = sg_config::SiteConfiguration::parse(&format!(
        r#"{{"encryption.keys": {{"externalServiceKey": {{"type": "mounted",
            "keyname": "extsvc", "filepath": "{}"}}}}}}"#,
        path.display()
    ))
    .expect("site configuration");
    let keyring = sg_keyring::init(&site).await.expect("keyring");
    let key = keyring
        .external_service_key()
        .expect("external service key");

    let plaintext = format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#);
    let ciphertext = key
        .encrypt(plaintext.as_bytes())
        .await
        .expect("encrypt config");
    let key_id = key.version().await.expect("key version").json();
    let ciphertext = String::from_utf8(ciphertext).expect("base64 envelope");

    let insert_encrypted = |config: String, key_id: String| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar(
                "INSERT INTO external_services(kind, display_name, config, encryption_key_id, \
                 unrestricted) VALUES ('OTHER', 'other-sync-test', $1, $2, true) RETURNING id",
            )
            .bind(config)
            .bind(key_id)
            .fetch_one(&pool)
            .await
            .expect("insert encrypted external service")
        }
    };

    // The configured key decrypts the stored envelope.
    let service_id = insert_encrypted(ciphertext.clone(), key_id.clone()).await;
    let (keyed, _diffs) = syncer_with(&pool, Arc::clone(&keyring));
    keyed
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &NoopProgressRecorder,
            &CancellationToken::new(),
        )
        .await
        .expect("encrypted config syncs");
    let names = repo_names(&pool, &suffix).await;
    assert_eq!(names, vec![(format!("{suffix}.example.com/r1"), false)]);

    // Without a key in site-config the sync fails before touching the source.
    let unkeyed_id = insert_encrypted(ciphertext, key_id).await;
    let (unkeyed, _diffs) = syncer(&pool);
    let err = unkeyed
        .sync_external_service(
            unkeyed_id,
            Duration::from_secs(60),
            &NoopProgressRecorder,
            &CancellationToken::new(),
        )
        .await
        .expect_err("encrypted config without a key");
    assert_eq!(
        err,
        format!(
            "external service id={unkeyed_id} config error: key mismatch: value is encrypted \
             but no encryption key available in site-config"
        )
    );

    cleanup(&pool, service_id, &suffix).await;
    cleanup(&pool, unkeyed_id, &suffix).await;
    let _ = std::fs::remove_dir_all(&dir);
}
