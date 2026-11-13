//! M9.31d differential of the GitHub App install/create HTTP API against the
//! real Go baseline (`cmd/frontend/internal/githubapp/zz_m931d_http_oracle_test.go`,
//! run by `run_githubapp_http_diff.sh`).
//!
//! The Go oracle drives the real handlers through gorilla/mux with a real
//! `rcache` (Redis) and Postgres store, replacing only the manifest conversion
//! (`MockCreateGitHubApp`). This harness replays the same corpus through
//! [`sg_service_frontend::githubapp_http_api::GitHubAppServer`] with in-memory
//! seams that implement the same store validation, a recording HTTP doer
//! (taking the conversion response from `mockApp`), a seeded state generator
//! and a recording sleeper (so the 3s wait is skipped).
//!
//! Env:
//!
//! * `SG_M931D_GITHUBAPP_HTTP_DIFF=1`
//! * `M931D_CORPUS` — `corpus.json`
//! * `M931D_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use http::{header, Method, Request};
use regex::Regex;
use serde::{Deserialize, Serialize};

use sg_service_frontend::githubapp_http_api::{
    GitHubAppRoute, GitHubAppServer, HttpDoer, MemoryGitHubApps, MemoryStateStore,
    MemoryWebhooks, SiteAdminCheck, SiteAdminOutcome, Sleeper, StateGenerator, StateStore,
};

#[derive(Deserialize, Clone)]
struct MockApp {
    #[serde(rename = "appID")]
    app_id: i32,
    name: String,
    slug: String,
    #[serde(rename = "appURL")]
    app_url: String,
    #[serde(rename = "baseURL")]
    base_url: String,
    #[serde(rename = "webhookSecret")]
    webhook_secret: String,
    #[serde(rename = "clientID")]
    client_id: String,
    #[serde(rename = "clientSecret")]
    client_secret: String,
    #[serde(rename = "privateKey")]
    private_key: String,
}

#[derive(Deserialize, Clone)]
struct SeedState {
    state: String,
    value: String,
}

#[derive(Deserialize, Clone)]
struct Case {
    name: String,
    route: String,
    #[serde(default)]
    query: String,
    actor: String,
    #[serde(rename = "seedState", default)]
    seed_state: Option<SeedState>,
    #[serde(rename = "mockApp", default)]
    mock_app: Option<MockApp>,
    #[serde(rename = "mockAppError", default)]
    mock_app_error: String,
}

#[derive(Deserialize)]
struct Fixtures {
    apps: Vec<FixtureApp>,
    webhooks: Vec<String>,
}

#[derive(Deserialize)]
struct FixtureApp {
    id: i32,
    name: String,
    domain: String,
    kind: String,
    #[serde(rename = "baseURL")]
    base_url: String,
}

#[derive(Deserialize)]
struct Corpus {
    fixtures: Fixtures,
    cases: Vec<Case>,
}

#[derive(Serialize)]
struct Output {
    status: u16,
    location: String,
    body: String,
    #[serde(rename = "conversionURL")]
    conversion_url: String,
}

struct FixedStateGenerator {
    counter: Mutex<u64>,
}

impl StateGenerator for FixedStateGenerator {
    fn generate(&self) -> Result<String, String> {
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        Ok(format!("{:064x}", 0x9000_0000_0000_0000u64 + *counter))
    }
}

struct RecordingSleeper {
    calls: Mutex<Vec<u64>>,
}

impl Sleeper for RecordingSleeper {
    fn sleep(&self, duration: std::time::Duration) {
        self.calls.lock().unwrap().push(duration.as_secs());
    }
}

struct RecordingHttp {
    mock_app: Option<MockApp>,
    error: String,
    seen: Mutex<String>,
}

impl HttpDoer for RecordingHttp {
    fn post(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        *self.seen.lock().unwrap() = url.to_owned();
        if !self.error.is_empty() {
            return Err(self.error.clone());
        }
        let mock = self.mock_app.as_ref().expect("mock app or error");
        let body = serde_json::json!({
            "id": mock.app_id,
            "slug": mock.slug,
            "name": mock.name,
            "html_url": mock.app_url,
            "client_id": mock.client_id,
            "client_secret": mock.client_secret,
            "pem": mock.private_key,
            "webhook_secret": mock.webhook_secret,
        });
        Ok((201, serde_json::to_vec(&body).unwrap()))
    }
}

struct ActorSiteAdmin {
    outcome: SiteAdminOutcome,
}

impl SiteAdminCheck for ActorSiteAdmin {
    fn check(&self) -> SiteAdminOutcome {
        self.outcome
    }
}

fn actor_outcome(actor: &str) -> SiteAdminOutcome {
    match actor {
        "nonadmin" => SiteAdminOutcome::Forbidden,
        "anonymous" => SiteAdminOutcome::Unauthenticated,
        _ => SiteAdminOutcome::Allowed,
    }
}

fn route(route: &str) -> GitHubAppRoute {
    match route {
        "state" => GitHubAppRoute::State,
        "new-app-state" => GitHubAppRoute::NewAppState,
        "redirect" => GitHubAppRoute::Redirect,
        "setup" => GitHubAppRoute::Setup,
        other => panic!("unknown route {other}"),
    }
}

fn normalize(value: &str) -> String {
    let uuid = Regex::new(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    )
    .unwrap();
    let state = Regex::new(r"[0-9a-f]{64}").unwrap();
    let value = uuid.replace_all(value, "<uuid>");
    state.replace_all(&value, "<state>").into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn githubapp_http_diff() {
    if std::env::var("SG_M931D_GITHUBAPP_HTTP_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M931D_CORPUS").expect("M931D_CORPUS");
    let out_dir = std::env::var("M931D_OUT").expect("M931D_OUT");
    std::fs::create_dir_all(&out_dir).unwrap();
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus_path).unwrap()).unwrap();

    let seed_apps: Vec<(i32, String, String, String, String)> = corpus
        .fixtures
        .apps
        .iter()
        .map(|app| {
            (
                app.id,
                app.name.clone(),
                app.domain.clone(),
                app.kind.clone(),
                app.base_url.clone(),
            )
        })
        .collect();
    let seed_webhooks = corpus.fixtures.webhooks.clone();

    for case in &corpus.cases {
        let cache = Arc::new(MemoryStateStore::new());
        let apps = Arc::new(MemoryGitHubApps::new());
        for (id, name, domain, kind, base_url) in &seed_apps {
            apps.insert(*id, name, domain, kind, base_url);
        }
        let webhooks = Arc::new(MemoryWebhooks::new());
        for uuid in &seed_webhooks {
            webhooks.insert(uuid, "seed-webhook");
        }
        let http = Arc::new(RecordingHttp {
            mock_app: case.mock_app.clone(),
            error: case.mock_app_error.clone(),
            seen: Mutex::new(String::new()),
        });
        let sleeper = Arc::new(RecordingSleeper {
            calls: Mutex::new(Vec::new()),
        });
        let server = GitHubAppServer {
            cache: cache.clone(),
            apps,
            webhooks,
            http: http.clone(),
            sleeper: sleeper.clone(),
            state: Arc::new(FixedStateGenerator {
                counter: Mutex::new(0),
            }),
            site_admin: Arc::new(ActorSiteAdmin {
                outcome: actor_outcome(&case.actor),
            }),
        };

        if let Some(seed) = &case.seed_state {
            cache.set(&seed.state, seed.value.clone().into_bytes());
        }

        let mut target = format!("/githubapp/{}", case.route);
        if !case.query.is_empty() {
            target.push('?');
            target.push_str(&case.query);
        }
        let request = Request::builder()
            .method(Method::GET)
            .uri(target)
            .body(())
            .unwrap();
        let response = server.serve(&request, route(&case.route));
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let body_bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let body = String::from_utf8_lossy(&body_bytes).into_owned();
        let conversion_url = http.seen.lock().unwrap().clone();

        let output = Output {
            status,
            location: normalize(&location),
            body: normalize(&body),
            conversion_url: normalize(&conversion_url),
        };
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            serde_json::to_vec(&output).unwrap(),
        )
        .unwrap();
        println!("RUST {} status={status}", case.name);
    }
}
