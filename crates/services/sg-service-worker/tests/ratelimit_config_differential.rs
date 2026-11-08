#![forbid(unsafe_code)]

//! M9.8i differential harness (Rust side): the `rate-limit-config` job.
//!
//! Runs `handler.Handle` once against the scratch frontend database
//! (`SG_QTEST_DSN`, `external_services_fixtures.sql` applied by the script) and
//! a diff-only Redis prefix, and prints the per-service error items. The
//! script dumps every `__diff_rate_limiters:*` key before and after so the
//! bucket configurations themselves are diffed too.
//!
//! `SG_HARNESS_SITE_JSON` is the site configuration JSON (the `gitMaxCodehost…`
//! cases); the Go counterpart is `cmd/worker/internal/ratelimit/harness_test.go`.
//! Both are gated by `SG_HARNESS_ON=1` so a plain `cargo test` skips them.

use std::sync::Arc;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_db::external_services::ExternalServiceStore;
use sg_keyring::Keyring;
use sg_service_worker::jobs::ratelimit::{redis_rate_limiter_factory, RateLimitConfigHandler};

/// The baseline's `ratelimit.SetupForTest` namespacing.
const DIFF_PREFIX: &str = "__diff_rate_limiters";

fn harness_enabled() -> bool {
    std::env::var("SG_HARNESS_ON").as_deref() == Ok("1")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limit_config_harness() {
    if !harness_enabled() {
        return;
    }
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set");
        return;
    };
    let redis = std::env::var("SG_QTEST_REDIS").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let site_json = std::env::var("SG_HARNESS_SITE_JSON").unwrap_or_default();

    let unified = Unified {
        site: SiteConfiguration::parse(&site_json).expect("parse SG_HARNESS_SITE_JSON"),
        service_connections: ServiceConnections::default(),
    };

    let pool = sg_db::connect(&dsn, 2).await.expect("connect database");
    let handler = RateLimitConfigHandler::new(
        ExternalServiceStore::new(pool),
        Arc::new(Keyring::default()),
        Arc::new(LiveConfig::new(unified)),
        redis_rate_limiter_factory(redis, DIFF_PREFIX.to_owned()),
    );

    match handler.handle_once().await {
        Ok(()) => println!("handle_error_count=0"),
        Err(err) => {
            println!("handle_error_count={}", err.0.len());
            for (index, item) in err.0.iter().enumerate() {
                println!("handle_error_{index}={item}");
            }
        }
    }
}
