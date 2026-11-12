//! M9.18c differential harness: drop-in startup surface.
//!
//! Emits the same normalized JSON as the Go oracle
//! `cmd/frontend/internal/cli/zz_m918c_dropin_oracle_test.go` (run once with
//! `INSECURE_DEV` unset and once set) plus the OSS GraphQL introspection
//! digest (`cmd/m9faschemajson`). Covers default listen addresses, `/healthz`
//! and `/__version`, the `tryAutoUpgrade` no-op gate, and the introspection
//! bytes.
//!
//! Gated on `SG_M918C_DIFF_HARNESS=1` plus `M918C_CORPUS`/`M918C_RUST_OUT`.

use std::sync::Mutex;

use bytes::Bytes;
use http::StatusCode;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sg_core::EnvReader;
use sg_service_frontend::config::FrontendConfig;
use sg_service_frontend::health::health_check;
use sg_service_frontend::service::{auto_upgrade_plan, AutoUpgradePlan, AutoUpgradeSkip};
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
struct PortCase {
    name: String,
    service: String,
    insecure_dev: bool,
}

#[derive(Deserialize)]
struct HealthCase {
    name: String,
    #[allow(dead_code)]
    service: String,
    path: String,
}

#[derive(Deserialize)]
struct AutoUpgradeCase {
    name: String,
    state: String,
    current: String,
    db: bool,
    target: String,
    env: bool,
}

#[derive(Deserialize)]
struct Corpus {
    ports: Vec<PortCase>,
    health: Vec<HealthCase>,
    autoupgrade: Vec<AutoUpgradeCase>,
}

#[derive(Serialize)]
struct PortOut {
    name: String,
    insecure_dev: bool,
    addr: String,
}

#[derive(Serialize)]
struct HealthOut {
    name: String,
    path: String,
    present: bool,
    status: u16,
    body: String,
}

#[derive(Serialize)]
struct AutoUpgradeOut {
    name: String,
    decision: String,
}

#[derive(Serialize)]
struct IntrospectionOut {
    len: usize,
    sha256: String,
}

#[derive(Serialize)]
struct Output {
    ports: Vec<PortOut>,
    health: Vec<HealthOut>,
    autoupgrade: Vec<AutoUpgradeOut>,
    introspection: IntrospectionOut,
}

const ENV_KEYS: [&str; 8] = [
    "INSECURE_DEV",
    "SRC_HTTP_ADDR",
    "SRC_HTTP_ADDR_INTERNAL",
    "SRC_PROF_HTTP",
    "GITSERVER_ADDR",
    "SEARCHER_ADDR",
    "SRC_REPOS_DIR",
    "SRC_AUTOUPGRADE",
];

fn with_env<T>(insecure_dev: bool, f: impl FnOnce() -> T) -> T {
    static LOCK: Mutex<()> = Mutex::new(());
    let _guard = LOCK.lock().unwrap_or_else(|error| error.into_inner());
    for key in ENV_KEYS {
        std::env::remove_var(key);
    }
    if insecure_dev {
        std::env::set_var("INSECURE_DEV", "true");
    }
    let value = f();
    for key in ENV_KEYS {
        std::env::remove_var(key);
    }
    value
}

fn frontend_addr(dev: bool, internal: bool) -> String {
    with_env(dev, || {
        let mut env = EnvReader::new();
        let config = FrontendConfig::load(&mut env).expect("frontend config");
        if internal {
            config.internal_bind_address().unwrap_or_default().to_owned()
        } else {
            config.external_bind_address().to_owned()
        }
    })
}

fn gitserver_addr(dev: bool) -> String {
    with_env(dev, || {
        let mut env = EnvReader::new();
        let config = sg_service_gitserver::config::GitserverConfig::load(&mut env, None)
            .expect("gitserver config");
        config.listen_addr
    })
}

fn searcher_addr(dev: bool) -> String {
    with_env(dev, || {
        let mut env = EnvReader::new();
        let config = sg_service_searcher::config::SearcherConfig::load(&mut env)
            .expect("searcher config");
        config.listen_address
    })
}

fn port_addr(case: &PortCase) -> String {
    match case.service.as_str() {
        "frontend" => frontend_addr(case.insecure_dev, case.name == "frontend.internal"),
        "gitserver" => gitserver_addr(case.insecure_dev),
        "searcher" => searcher_addr(case.insecure_dev),
        // `crates/services/sg-service-repo-updater/src/config.rs:7-8,36-47`.
        "repo-updater" => {
            if case.insecure_dev {
                "127.0.0.1:3182".to_owned()
            } else {
                ":3182".to_owned()
            }
        }
        // `crates/services/sg-service-worker/src/config.rs:26-27`.
        "worker" => ":3189".to_owned(),
        // `cmd/symbols/shared/main.go:47` (`addr = ":3184"`); symbols is not a
        // dependency of this crate.
        "symbols" => ":3184".to_owned(),
        // `cmd/precise-code-intel-worker/shared/shared.go:32` (`addr = ":3188"`).
        "precise-code-intel-worker" => ":3188".to_owned(),
        other => panic!("unknown service {other}"),
    }
}

fn decision(plan: Result<AutoUpgradePlan, String>) -> &'static str {
    match plan {
        Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::FreshInstance)) => "skip:fresh-instance",
        Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::Disabled)) => "skip:disabled",
        Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::SameVersionOrNewer)) => {
            "skip:same-version-or-newer"
        }
        Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::UnexpectedTargetVersion)) => {
            "skip:unexpected-target-version"
        }
        Ok(AutoUpgradePlan::Required { .. }) => "required",
        Err(_) => "error:unexpected-current-version",
    }
}

#[tokio::test]
async fn dump_dropin_acceptance() {
    if std::env::var("SG_M918C_DIFF_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M918C_CORPUS").expect("M918C_CORPUS");
    let output_path = std::env::var("M918C_RUST_OUT").expect("M918C_RUST_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let ports: Vec<PortOut> = corpus
        .ports
        .iter()
        .map(|case| PortOut {
            name: case.name.clone(),
            insecure_dev: case.insecure_dev,
            addr: port_addr(case),
        })
        .collect();

    let version = Bytes::from_static(b"0.0.0+dev");
    let mut health = Vec::new();
    for case in &corpus.health {
        match health_check(&case.path, &version) {
            Some(response) => {
                let status = response.status().as_u16();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .expect("health body")
                    .to_bytes();
                assert_eq!(status, StatusCode::OK.as_u16());
                health.push(HealthOut {
                    name: case.name.clone(),
                    path: case.path.clone(),
                    present: true,
                    status,
                    body: String::from_utf8_lossy(&body).into_owned(),
                });
            }
            None => health.push(HealthOut {
                name: case.name.clone(),
                path: case.path.clone(),
                present: false,
                status: 0,
                body: String::new(),
            }),
        }
    }

    let autoupgrade: Vec<AutoUpgradeOut> = corpus
        .autoupgrade
        .iter()
        .map(|case| {
            let state = if case.state == "fresh" {
                None
            } else {
                Some((case.current.clone(), case.db))
            };
            let plan = auto_upgrade_plan(state, &case.target, case.env);
            AutoUpgradeOut {
                name: case.name.clone(),
                decision: decision(plan).to_owned(),
            }
        })
        .collect();

    let schema = sg_graphql::schema::assemble_oss().expect("OSS schema");
    let json = schema.to_json();
    let digest = Sha256::digest(json.as_bytes());
    let sha256: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();

    let output = Output {
        ports,
        health,
        autoupgrade,
        introspection: IntrospectionOut {
            len: json.len(),
            sha256,
        },
    };
    std::fs::write(output_path, serde_json::to_vec(&output).expect("serialize"))
        .expect("write output");
}
