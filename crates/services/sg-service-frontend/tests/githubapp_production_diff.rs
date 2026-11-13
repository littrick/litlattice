//! M9.32c differential: the *production* GitHub App install/create wiring
//! (`GitHubAppServer::production`) against the Go baseline
//! (`cmd/frontend/internal/githubapp/zz_m932c_production_oracle_test.go`, run
//! by `run_githubapp_production_diff.sh`).
//!
//! Both sides use a real Postgres (`github_apps`/`webhooks`), a real Redis
//! (`rcache`, `github_app_state`) and a real HTTP client. The manifest
//! conversion endpoint is an in-process fake GitHub server whose authority is
//! substituted into the seeded state's `base_url`; each case additionally
//! records the post-request `github_apps`/`webhooks` snapshot.
//!
//! Env:
//!
//! * `SG_M932C_GITHUBAPP_PRODUCTION_DIFF=1`
//! * `M932C_CORPUS` — `corpus.json`
//! * `M932C_OUT` — output directory (one `<name>.json` per case)
//! * `SG_QTEST_DSN` — scratch database DSN
//! * `M932C_REDIS` — Redis endpoint (default `127.0.0.1:6379`)

#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use http::{header, Method, Request};
use regex::Regex;
use serde::{Deserialize, Serialize};

use sg_service_frontend::githubapp_http_api::{
    route, AllowAllSiteAdmin, GitHubAppServer, PgGitHubAppStore, PgWebhookBackend,
    RcacheStateStore, ReqwestHttpDoer, Sleeper, SystemStateGenerator,
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
    #[allow(dead_code)]
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
struct AppRow {
    id: i32,
    #[serde(rename = "appID")]
    app_id: i32,
    name: String,
    domain: String,
    kind: String,
    #[serde(rename = "baseURL")]
    base_url: String,
    #[serde(rename = "appURL")]
    app_url: String,
    logo: String,
    #[serde(rename = "clientID")]
    client_id: String,
    #[serde(rename = "clientSecret")]
    client_secret: String,
    #[serde(rename = "privateKey")]
    private_key: String,
    #[serde(rename = "encryptionKeyID")]
    encryption_key_id: String,
    #[serde(rename = "creatorID")]
    creator_id: i32,
}

#[derive(Serialize)]
struct WebhookRow {
    uuid: String,
    name: String,
    #[serde(rename = "codeHostKind")]
    code_host_kind: String,
    #[serde(rename = "codeHostURN")]
    code_host_urn: String,
    secret: String,
    #[serde(rename = "encryptionKeyID")]
    encryption_key_id: String,
}

#[derive(Serialize)]
struct Output {
    status: u16,
    location: String,
    body: String,
    #[serde(rename = "conversionURL")]
    conversion_url: String,
    apps: Vec<AppRow>,
    webhooks: Vec<WebhookRow>,
}

struct NoopSleeper;

impl Sleeper for NoopSleeper {
    fn sleep(&self, _duration: std::time::Duration) {}
}

/// A single-connection-at-a-time fake GitHub manifest endpoint. The response
/// and the last request path are shared with the test thread.
struct FakeGithub {
    authority: String,
    response: Arc<Mutex<(u16, Vec<u8>)>>,
    last_path: Arc<Mutex<String>>,
}

impl FakeGithub {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let authority = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let response = Arc::new(Mutex::new((500u16, Vec::<u8>::new())));
        let last_path = Arc::new(Mutex::new(String::new()));
        let response_for_thread = Arc::clone(&response);
        let path_for_thread = Arc::clone(&last_path);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);
                            if buffer.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                if let Some(line_end) = buffer.windows(2).position(|w| w == b"\r\n") {
                    let line = String::from_utf8_lossy(&buffer[..line_end]);
                    if let Some(path) = line.split_whitespace().nth(1) {
                        *path_for_thread.lock().unwrap() = path.to_owned();
                    }
                }
                let (status, body) = response_for_thread.lock().unwrap().clone();
                let reason = if status == 201 {
                    "Created"
                } else {
                    "Internal Server Error"
                };
                let head = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        Self {
            authority,
            response,
            last_path,
        }
    }

    fn set_response(&self, status: u16, body: Vec<u8>) {
        *self.response.lock().unwrap() = (status, body);
    }

    fn take_path(&self) -> String {
        std::mem::take(&mut *self.last_path.lock().unwrap())
    }
}

fn normalize(value: &str, authority: &str) -> String {
    let mut text = if authority.is_empty() {
        value.to_owned()
    } else {
        value.replace(authority, "http://github.test")
    };
    let uuid = Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").unwrap();
    text = uuid.replace_all(&text, "<uuid>").into_owned();
    let state = Regex::new(r"[0-9a-f]{64}").unwrap();
    state.replace_all(&text, "<state>").into_owned()
}

async fn seed_database(pool: &sqlx::PgPool, corpus: &Corpus) {
    for query in [
        "DELETE FROM github_app_installs",
        "DELETE FROM github_apps",
        "DELETE FROM webhooks",
        "DELETE FROM users WHERE id IN (101, 102)",
        "INSERT INTO users (id, username, site_admin) VALUES \
             (101, 'm932cadmin', true), (102, 'm932cuser', false)",
    ] {
        sqlx::query(query).execute(pool).await.unwrap();
    }
    for app in &corpus.fixtures.apps {
        sqlx::query(
            "INSERT INTO github_apps \
                 (id, app_id, name, slug, base_url, client_id, client_secret, private_key, \
                  encryption_key_id, logo, app_url, domain, kind, creator_id) \
             VALUES ($1, $2, $3, $3, $4, 'cid', 'csec', 'pem', '', 'logo', $5, \
                     $6, $7::github_app_kind, 0)",
        )
        .bind(app.id)
        .bind(app.id * 100)
        .bind(&app.name)
        .bind(&app.base_url)
        .bind(format!("https://example.com/apps/{}", app.name))
        .bind(&app.domain)
        .bind(&app.kind)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query("SELECT setval('github_apps_id_seq', (SELECT COALESCE(MAX(id), 1) FROM github_apps))")
        .execute(pool)
        .await
        .unwrap();
    for uuid in &corpus.fixtures.webhooks {
        sqlx::query(
            "INSERT INTO webhooks (uuid, code_host_kind, code_host_urn, name) \
             VALUES ($1::uuid, 'GITHUB', 'https://ghe.example.com', 'seed-webhook')",
        )
        .bind(uuid)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn snapshot(
    pool: &sqlx::PgPool,
    authority: &str,
) -> (Vec<AppRow>, Vec<WebhookRow>) {
    use sqlx::Row;
    let app_rows = sqlx::query(
        "SELECT id, app_id, name, COALESCE(domain::text,'') AS domain, COALESCE(kind::text,'') AS kind, \
                COALESCE(base_url,'') AS base_url, COALESCE(app_url,'') AS app_url, \
                COALESCE(logo,'') AS logo, COALESCE(client_id,'') AS client_id, \
                COALESCE(client_secret,'') AS client_secret, COALESCE(private_key,'') AS private_key, \
                COALESCE(encryption_key_id,'') AS encryption_key_id, COALESCE(creator_id,0)::int4 AS creator_id \
         FROM github_apps ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let apps = app_rows
        .iter()
        .map(|row| AppRow {
            id: row.get("id"),
            app_id: row.get("app_id"),
            name: normalize(row.get::<String, _>("name").as_str(), authority),
            domain: row.get("domain"),
            kind: row.get("kind"),
            base_url: normalize(row.get::<String, _>("base_url").as_str(), authority),
            app_url: normalize(row.get::<String, _>("app_url").as_str(), authority),
            logo: normalize(row.get::<String, _>("logo").as_str(), authority),
            client_id: row.get("client_id"),
            client_secret: row.get("client_secret"),
            private_key: row.get("private_key"),
            encryption_key_id: row.get("encryption_key_id"),
            creator_id: row.get("creator_id"),
        })
        .collect();

    let webhook_rows = sqlx::query(
        "SELECT uuid::text AS uuid, COALESCE(name,'') AS name, \
                COALESCE(code_host_kind,'') AS code_host_kind, \
                COALESCE(code_host_urn,'') AS code_host_urn, \
                COALESCE(secret,'') AS secret, \
                COALESCE(encryption_key_id,'') AS encryption_key_id \
         FROM webhooks",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let mut webhooks: Vec<WebhookRow> = webhook_rows
        .iter()
        .map(|row| WebhookRow {
            uuid: normalize(row.get::<String, _>("uuid").as_str(), authority),
            name: normalize(row.get::<String, _>("name").as_str(), authority),
            code_host_kind: row.get("code_host_kind"),
            code_host_urn: normalize(row.get::<String, _>("code_host_urn").as_str(), authority),
            secret: normalize(row.get::<String, _>("secret").as_str(), authority),
            encryption_key_id: row.get("encryption_key_id"),
        })
        .collect();
    webhooks.sort_by(|a, b| (a.uuid.clone(), a.name.clone()).cmp(&(b.uuid.clone(), b.name.clone())));
    (apps, webhooks)
}

#[tokio::test(flavor = "multi_thread")]
async fn githubapp_production_diff() {
    if std::env::var("SG_M932C_GITHUBAPP_PRODUCTION_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M932C_CORPUS").expect("M932C_CORPUS");
    let out_dir = std::env::var("M932C_OUT").expect("M932C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let redis = std::env::var("M932C_REDIS").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    std::fs::create_dir_all(&out_dir).unwrap();
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).unwrap()).unwrap();

    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect postgres");
    seed_database(&pool, &corpus).await;

    let kv = sg_redis::KeyValue::connect(&redis)
        .await
        .expect("connect redis");
    let keyring = Arc::new(sg_keyring::Keyring::default());
    let fake = FakeGithub::start();

    let server = GitHubAppServer {
        cache: Arc::new(RcacheStateStore::with_kv(kv)),
        apps: Arc::new(PgGitHubAppStore::new(pool.clone(), keyring.clone())),
        webhooks: Arc::new(PgWebhookBackend::new(pool.clone(), keyring, 0)),
        http: Arc::new(ReqwestHttpDoer::new()),
        sleeper: Arc::new(NoopSleeper),
        state: Arc::new(SystemStateGenerator),
        site_admin: Arc::new(AllowAllSiteAdmin),
    };

    for case in &corpus.cases {
        let uses_http = case.mock_app.is_some() || !case.mock_app_error.is_empty();
        if uses_http {
            if !case.mock_app_error.is_empty() {
                fake.set_response(500, Vec::new());
            } else {
                let mock = case.mock_app.as_ref().unwrap();
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
                fake.set_response(201, serde_json::to_vec(&body).unwrap());
            }
        } else {
            fake.set_response(500, Vec::new());
        }
        let _ = fake.take_path();

        if let Some(seed) = &case.seed_state {
            let value = if uses_http {
                match serde_json::from_str::<serde_json::Value>(&seed.value) {
                    Ok(mut details) => {
                        details["base_url"] = serde_json::Value::String(fake.authority.clone());
                        serde_json::to_string(&details).unwrap()
                    }
                    Err(_) => seed.value.clone(),
                }
            } else {
                seed.value.clone()
            };
            server.cache.set(&seed.state, value.into_bytes());
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
        let response = server.serve(&request, route(&format!("/githubapp/{}", case.route)).unwrap());
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

        let path = fake.take_path();
        let authority = if uses_http { fake.authority.as_str() } else { "" };
        let conversion_url = if path.is_empty() {
            String::new()
        } else {
            format!("{}{}", fake.authority, path)
        };

        let (apps, webhooks) = snapshot(&pool, authority).await;
        let output = Output {
            status,
            location: normalize(&location, authority),
            body: normalize(&body, authority),
            conversion_url: normalize(&conversion_url, authority),
            apps,
            webhooks,
        };
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            serde_json::to_vec(&output).unwrap(),
        )
        .unwrap();
        println!("RUST {} status={status}", case.name);
    }
}
