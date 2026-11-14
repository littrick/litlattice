//! M9.40d differential harness: worker search assembly with the keyring
//! `UserExternalAccountKey` and the live configuration.
//!
//! Go oracle (UNTRACKED, gated by `SG_M940D_WORKER_SEARCH_ASSEMBLY_DIFF=1`):
//! `internal/search/zz_m940d_oracle/zz_m940d_worker_search_assembly_oracle_test.go`.
//!
//! Cases:
//!
//! * `keyring` — whether `keyring.Default().UserExternalAccountKey` exists
//!   for a `encryption.keys` object;
//! * `decrypt` — Go encrypts the plaintext with a mounted key and writes the
//!   raw ciphertext next to `M940D_CIPHER_DIR`; Rust builds the same ring and
//!   decrypts it (cross-language envelope check);
//! * `assembly` — the worker wiring: `assemble_{exhaustive,commit_search}_
//!   with_encryption_key` reports the injected key, `max_repos_from_site`
//!   and `exhaustive_search_job::is_enabled` match the Go oracle's
//!   `limits.SearchLimits` / `exhaustive.IsEnabled`.
//!
//! Env:
//!
//! * `SG_M940D_WORKER_SEARCH_ASSEMBLY_DIFF=1` enable
//! * `M940D_CORPUS` corpus JSON
//! * `M940D_OUT` output JSON
//! * `M940D_CIPHER_DIR` directory with the Go-produced `<case>.bin` files

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPool;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};

#[derive(Debug, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    keys: Option<Value>,
    #[serde(default)]
    site: Option<Value>,
    #[serde(default)]
    variant: String,
}

/// A pool that never connects: assembly only builds lazy clients/stores.
fn lazy_pool() -> PgPool {
    PgPool::connect_lazy("postgres://invalid:invalid@127.0.0.1:1/none")
        .expect("lazy pg pool")
}

/// `conf.ParseConfig` for a corpus `encryption.keys` object.
fn site_with_keys(keys: Option<&Value>) -> SiteConfiguration {
    match keys {
        Some(keys) if !keys.is_null() => {
            let raw = format!(r#"{{"encryption.keys":{keys}}}"#);
            SiteConfiguration::parse(&raw).expect("parse encryption keys")
        }
        _ => SiteConfiguration::default(),
    }
}

fn site_from_case(case: &Case) -> SiteConfiguration {
    match &case.site {
        Some(site) if !site.is_null() => {
            SiteConfiguration::parse(&site.to_string()).expect("parse site config")
        }
        _ => SiteConfiguration::default(),
    }
}

async fn describe_keyring(case: &Case) -> Value {
    let site = site_with_keys(case.keys.as_ref());
    let present = match sg_keyring::init(&site).await {
        Ok(keyring) => keyring.user_external_account_key().is_some(),
        Err(_) => false,
    };
    json!({
        "kind": "keyring",
        "name": case.name,
        "user_external_account_key_present": present,
    })
}

async fn describe_decrypt(case: &Case) -> Value {
    let site = site_with_keys(case.keys.as_ref());
    let cipher_dir = std::env::var("M940D_CIPHER_DIR").expect("M940D_CIPHER_DIR");
    let cipher_path = format!("{cipher_dir}/{}.bin", case.name);
    let ciphertext = std::fs::read(&cipher_path).expect("read Go ciphertext");
    let key = sg_keyring::init(&site)
        .await
        .expect("init keyring")
        .user_external_account_key();
    let decrypted = match key {
        Some(key) => key
            .decrypt(&ciphertext)
            .await
            .ok()
            .map(|secret| secret.secret().to_owned()),
        None => None,
    };
    json!({
        "kind": "decrypt",
        "name": case.name,
        "ok": decrypted.is_some(),
        "plaintext": decrypted,
    })
}

fn assembly_descriptor(case: &Case) -> Value {
    let site = site_from_case(case);
    let connections = ServiceConnections::default();
    let pool = lazy_pool();
    // In production the ring is built from the whole site config
    // (`encryption.keys` is one of its dotted top-level keys). The corpus
    // keeps `keys` separate for the keyring/decrypt cases, so build the ring
    // from that object and pass the resulting key into the assembly.
    let key = init_keyring_blocking(&site_with_keys(case.keys.as_ref()));

    let exhaustive_enabled = sg_service_worker::jobs::exhaustive_search_job::is_enabled(
        &LiveConfig::new(Unified {
            site: site.clone(),
            service_connections: connections.clone(),
        }),
    );
    let max_repos = sg_service_frontend::search_runtime::max_repos_from_site(&site);

    let (key_present, commit_present, searcher_present) = if case.variant == "commit_search" {
        let runtime =
            sg_service_frontend::search_runtime::assemble_commit_search_with_encryption_key(
                &pool,
                &connections,
                &site,
                key,
            );
        let key_present = runtime
            .commit
            .as_ref()
            .map(|commit| commit.user_external_account_key_present)
            .unwrap_or(false);
        (key_present, runtime.commit.is_some(), runtime.searcher.is_some())
    } else {
        let runtime = sg_service_frontend::search_runtime::assemble_exhaustive_with_encryption_key(
            &pool,
            &connections,
            &site,
            key,
        );
        let key_present = runtime
            .clients
            .commit
            .as_ref()
            .map(|commit| commit.user_external_account_key_present)
            .unwrap_or(false);
        (
            key_present,
            runtime.clients.commit.is_some(),
            runtime.clients.searcher.is_some(),
        )
    };

    json!({
        "kind": "assembly",
        "name": case.name,
        "variant": case.variant,
        "exhaustive_enabled": exhaustive_enabled,
        "max_repos": max_repos,
        "user_external_account_key_present": key_present,
        "commit_clients_present": commit_present,
        "searcher_clients_present": searcher_present,
    })
}

/// Synchronous keyring construction for the assembly cases (the harness runs
/// inside `#[tokio::test]`, but `assemble_*` is synchronous; `block_on` keeps
/// the key construction at the same call site as the Go oracle).
fn init_keyring_blocking(site: &SiteConfiguration) -> Option<Arc<dyn sg_encryption::Key>> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(sg_keyring::init(site))
            .expect("init keyring")
            .user_external_account_key()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_search_assembly_diff() {
    if std::env::var("SG_M940D_WORKER_SEARCH_ASSEMBLY_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M940D_CORPUS").expect("M940D_CORPUS");
    let out_path = std::env::var("M940D_OUT").expect("M940D_OUT");
    let raw = std::fs::read(&corpus_path).expect("read corpus");
    let corpus: Value = serde_json::from_slice(&raw).expect("parse corpus");
    let cases: Vec<Case> = serde_json::from_value(
        corpus
            .get("cases")
            .cloned()
            .expect("corpus cases"),
    )
    .expect("decode cases");

    let mut outputs = Vec::with_capacity(cases.len());
    for case in &cases {
        let output = match case.kind.as_str() {
            "keyring" => describe_keyring(case).await,
            "decrypt" => describe_decrypt(case).await,
            "assembly" => assembly_descriptor(case),
            other => panic!("unknown case kind {other:?}"),
        };
        outputs.push(output);
    }
    let bytes = serde_json::to_vec(&outputs).expect("serialize output");
    std::fs::write(&out_path, bytes).expect("write output");
}
