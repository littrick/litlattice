//! M9.8gd differential of `RepoSourceCloneURLToRepoName`
//! (`cmd/frontend/internal/cloneurls/clone_urls.go:26-91`) plus the
//! `internal/conf/reposource` `CloneURLToRepoName` implementations, backed by
//! the same scratch database rows as the Go oracle.
//!
//! Env:
//!
//! * `SG_M9GD_DIFF=1`
//! * `M9GD_CORPUS` — JSON `[{name, cloneURL, siteConfig?}]`
//! * `M9GD_OUT` — output directory (one `<name>.json` per case)
//! * `M9GD_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use sg_config::SiteConfiguration;
use sg_service_frontend::clone_urls::{
    repo_source_clone_url_to_repo_name, PgCloneUrlServiceSource, PgFirstRepoNameByCloneUrl,
};

/// Field order matches the Go oracle's `m9gdEnvelope` struct.
#[derive(Serialize)]
struct Envelope {
    #[serde(rename = "repoName")]
    repo_name: String,
    error: bool,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(rename = "cloneURL")]
    clone_url: String,
    #[serde(default, rename = "siteConfig")]
    site_config: Option<Value>,
}

#[tokio::test]
async fn clone_url_harness() {
    if std::env::var("SG_M9GD_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GD_CORPUS").expect("M9GD_CORPUS");
    let out = std::env::var("M9GD_OUT").expect("M9GD_OUT");
    let dsn = std::env::var("M9GD_DSN").expect("M9GD_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let first = PgFirstRepoNameByCloneUrl::new(pool.clone());
    let services = PgCloneUrlServiceSource::new(pool.clone());

    for case in cases {
        let site = match &case.site_config {
            Some(config) => SiteConfiguration::parse(&config.to_string()).expect("site config"),
            None => SiteConfiguration::default(),
        };
        let resolved =
            repo_source_clone_url_to_repo_name(&first, &services, &site, None, &case.clone_url)
                .await;
        let envelope = match resolved {
            Ok(name) => Envelope {
                repo_name: name.unwrap_or_default(),
                error: false,
            },
            Err(_) => Envelope {
                repo_name: String::new(),
                error: true,
            },
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
