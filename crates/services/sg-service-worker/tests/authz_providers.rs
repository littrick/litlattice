#![forbid(unsafe_code)]

//! M9.8av differential harness (Rust side):
//! `ProvidersFromConfig` for GitHub external services.
//!
//! Mirrors `cmd/worker/internal/authz/providers_harness_test.go`.
//! `test_ws/p9-go/run_authz_providers_diff.sh` applies
//! `authz_providers_fixtures.sql` immediately before each side.

use sg_config::SiteConfiguration;
use sg_keyring::Keyring;
use sg_service_worker::jobs::authz_providers::{providers_from_config, ProvidersOptions};
use sqlx::postgres::PgPool;

#[tokio::test(flavor = "multi_thread")]
async fn authz_providers_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let keyring = Keyring::default();

    let cases: [(&str, &str, Option<&str>); 5] = [
        ("plain", "{}", None),
        (
            "warn",
            r#"{"auth.providers":[{"type":"github","url":"https://github.example.com","clientID":"c","clientSecret":"s"}]}"#,
            None,
        ),
        (
            "allow",
            r#"{"auth.providers":[{"type":"github","url":"https://github.example.com","clientID":"c","clientSecret":"s","allowGroupsPermissionsSync":true}]}"#,
            None,
        ),
        (
            "defaulturl",
            r#"{"auth.providers":[{"type":"github","clientID":"c","clientSecret":"s"}]}"#,
            None,
        ),
        (
            "license",
            r#"{"auth.providers":[{"type":"github","url":"https://github.example.com","clientID":"c","clientSecret":"s"}]}"#,
            Some("acls unavailable"),
        ),
    ];

    for (name, site_json, license_error) in cases {
        let site = SiteConfiguration::parse(site_json).expect("site config");
        let opts = ProvidersOptions {
            acls_license_error: license_error.map(str::to_owned),
            enable_github_internal_repo_visibility: false,
        };
        let result = providers_from_config(&pool, &site, &keyring, &opts).await;

        println!(
            "ap scenario={name} providers={} problems={} warnings={} invalid={}",
            result.providers.len(),
            result.serious_problems.len(),
            result.warnings.len(),
            result.invalid_connections.len()
        );
        for (i, provider) in result.providers.iter().enumerate() {
            println!(
                "ap scenario={name} provider={i} urn={} type={} id={}",
                provider.urn, provider.service_type, provider.service_id
            );
        }
        for (i, problem) in result.serious_problems.iter().enumerate() {
            println!("ap scenario={name} problem={i} text={problem}");
        }
        for (i, warning) in result.warnings.iter().enumerate() {
            println!("ap scenario={name} warning={i} text={warning}");
        }
        for (i, conn) in result.invalid_connections.iter().enumerate() {
            println!("ap scenario={name} invalid={i} type={conn}");
        }
    }
}
